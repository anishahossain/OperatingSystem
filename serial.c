#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576      // 1MB, matches starter
#define MAX_THREADS 20           // project says: never > 20 (including main)

// ---------- sorting helper ----------
static int cmp(const void *a, const void *b) {
    return strcmp(*(char * const *) a, *(char * const *) b);
}

// ---------- job structure ----------
typedef struct {
    char  *full_path;            // "dir/file.txt"
    unsigned char *compressed;   // heap buffer holding compressed data
    int compressed_size;         // how many bytes are valid
    int original_size;           // bytes read from file
    int done;
} file_job_t;

// ---------- globals for workers ----------
static file_job_t *g_jobs = NULL;
static int g_nfiles = 0;

static pthread_mutex_t g_job_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_next_job = 0;

// ---------- worker thread ----------
static void *worker_thread(void *arg) {
    (void)arg;

    // allocate big scratch buffers ONCE per thread
    unsigned char *buffer_in  = malloc(BUFFER_SIZE);
    unsigned char *buffer_out = malloc(BUFFER_SIZE);
    assert(buffer_in && buffer_out);

    for (;;) {
        // get a job index
        pthread_mutex_lock(&g_job_lock);
        int idx = g_next_job++;
        pthread_mutex_unlock(&g_job_lock);

        if (idx >= g_nfiles) {
            break;  // no more files
        }

        file_job_t *job = &g_jobs[idx];

        // read file
        FILE *f_in = fopen(job->full_path, "rb");
        assert(f_in != NULL);
        int nbytes = (int)fread(buffer_in, 1, BUFFER_SIZE, f_in);
        fclose(f_in);

        job->original_size = nbytes;

        // compress into buffer_out
        z_stream strm;
        int ret = deflateInit(&strm, 9);
        assert(ret == Z_OK);

        strm.avail_in = nbytes;
        strm.next_in  = buffer_in;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out  = buffer_out;

        ret = deflate(&strm, Z_FINISH);
        assert(ret == Z_STREAM_END);

        int nbytes_zipped = (int)(BUFFER_SIZE - strm.avail_out);
        deflateEnd(&strm);

        // store result in a job-owned buffer
        unsigned char *out_copy = malloc(nbytes_zipped);
        assert(out_copy != NULL);
        memcpy(out_copy, buffer_out, nbytes_zipped);

        job->compressed      = out_copy;
        job->compressed_size = nbytes_zipped;
        job->done            = 1;
    }

    free(buffer_in);
    free(buffer_out);
    return NULL;
}

// ---------- main entry ----------
int compress_directory(char *directory_name) {
    DIR *d;
    struct dirent *dir;
    char **files = NULL;
    int nfiles = 0;

    d = opendir(directory_name);
    if (d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // collect txt files
    while ((dir = readdir(d)) != NULL) {
        int len = (int)strlen(dir->d_name);
        if (len >= 4 &&
            dir->d_name[len-4] == '.' &&
            dir->d_name[len-3] == 't' &&
            dir->d_name[len-2] == 'x' &&
            dir->d_name[len-1] == 't') {

            files = realloc(files, (nfiles + 1) * sizeof(char *));
            assert(files != NULL);
            files[nfiles] = strdup(dir->d_name);
            assert(files[nfiles] != NULL);
            nfiles++;
        }
    }
    closedir(d);

    // sort lexicographically
    qsort(files, nfiles, sizeof(char *), cmp);

    // build job array
    g_jobs = malloc(nfiles * sizeof(file_job_t));
    assert(g_jobs != NULL);
    g_nfiles = nfiles;

    for (int i = 0; i < nfiles; i++) {
        int len = (int)strlen(directory_name) + (int)strlen(files[i]) + 2;
        char *full_path = malloc(len);
        assert(full_path != NULL);
        strcpy(full_path, directory_name);
        strcat(full_path, "/");
        strcat(full_path, files[i]);

        g_jobs[i].full_path       = full_path;
        g_jobs[i].compressed      = NULL;
        g_jobs[i].compressed_size = 0;
        g_jobs[i].original_size   = 0;
        g_jobs[i].done            = 0;
    }

    // decide number of threads: up to 19 workers + 1 main = 20 total
    int num_threads = nfiles;
    if (num_threads > (MAX_THREADS - 1)) {  // leave 1 for main
        num_threads = MAX_THREADS - 1;
    }
    if (num_threads < 1) {
        num_threads = 1;
    }

    pthread_t threads[MAX_THREADS];

    // spawn workers
    for (int i = 0; i < num_threads; i++) {
        int rc = pthread_create(&threads[i], NULL, worker_thread, NULL);
        assert(rc == 0);
    }

    // wait for workers
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // write final archive in order
    int total_in = 0;
    int total_out = 0;
    FILE *f_out = fopen("text.tzip", "wb");
    assert(f_out != NULL);

    for (int i = 0; i < nfiles; i++) {
        fwrite(&g_jobs[i].compressed_size, sizeof(int), 1, f_out);
        fwrite(g_jobs[i].compressed, 1, g_jobs[i].compressed_size, f_out);

        total_in  += g_jobs[i].original_size;
        total_out += g_jobs[i].compressed_size;
    }

    fclose(f_out);

    if (total_in > 0) {
        printf("Compression rate: %.2lf%%\n",
               100.0 * (total_in - total_out) / total_in);
    } else {
        printf("Compression rate: 0.00%%\n");
    }

    // cleanup
    for (int i = 0; i < nfiles; i++) {
        free(g_jobs[i].full_path);
        free(g_jobs[i].compressed);
    }
    free(g_jobs);

    for (int i = 0; i < nfiles; i++) {
        free(files[i]);
    }
    free(files);

    return 0;
}
