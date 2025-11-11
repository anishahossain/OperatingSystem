// Project 2 - Parallel Text Compression
// Authors: Alina Pineda, Anisha Hossain, Brielle Ashmeade, Georgia Ng Wai
// Description:
// Parallelizes the compression of .txt files in a directory using pthreads.
// Each worker thread pulls the next file index, reads it, compresses it,
// and stores the result. The main thread then writes all compressed files
// in lexicographical order to "text.tzip".
//
// Constraints:
// - Only pthread library
// - At most 20 threads total (main + workers)
// - Output format must match the baseline (size + bytes per file)
// Notes:
// - This version focuses on simplicity and correctness.
// - Each worker owns its own I/O buffers to avoid repeated allocation.
// - The shared job index is protected by a mutex to ensure only one thread
//   claims a given file at a time, which preserves correctness even though
//   multiple threads are running.
// - Final output order is preserved because we still write using the
//   lexicographically sorted array of file names.

#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576      // 1 MB
#define MAX_THREADS 20           // main + workers must stay <= 20

// ---------- sorting helper ----------
// qsort comparator: sorts C strings (file names) in ascending lexicographical order
static int cmp(const void *a, const void *b) {
    return strcmp(*(char * const *) a, *(char * const *) b);
}

// ---------- job structure ----------
// Represents one file to compress and the data produced for it.
typedef struct {
    char  *full_path;            // "dir/file.txt"
    unsigned char *compressed;   // heap buffer holding compressed data
    int compressed_size;         // how many bytes are valid
    int original_size;           // bytes read from file
    int done;                    // optional flag to mark completion
} file_job_t;

// ---------- globals ----------
// Array of jobs, kept in the same order as the sorted file list.
static file_job_t *g_jobs = NULL;
static int g_nfiles = 0;

// Mutex + shared index to distribute work among threads.
static pthread_mutex_t g_job_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_next_job = 0;

// ---------- worker thread ----------
// Each worker repeatedly claims the next available job, compresses that file,
// and stores the result back into the corresponding job slot.
static void *worker_thread(void *arg) {
    (void)arg;

    // allocate per-thread scratch buffers (so we don't malloc per file)
    unsigned char *buffer_in  = malloc(BUFFER_SIZE);
    unsigned char *buffer_out = malloc(BUFFER_SIZE);
    assert(buffer_in && buffer_out);

    for (;;) {
        // get a job index
        pthread_mutex_lock(&g_job_lock);
        int idx = g_next_job++;
        pthread_mutex_unlock(&g_job_lock);

        if (idx >= g_nfiles)
            break;

        file_job_t *job = &g_jobs[idx];

        // read file
        FILE *f_in = fopen(job->full_path, "rb");
        assert(f_in != NULL);
        int nbytes = (int)fread(buffer_in, 1, BUFFER_SIZE, f_in);
        fclose(f_in);
        job->original_size = nbytes;

        // compress into buffer_out
        z_stream strm;
        int ret = deflateInit(&strm, 9);      // use highest compression like starter
        assert(ret == Z_OK);

        strm.avail_in  = nbytes;
        strm.next_in   = buffer_in;
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
// Scans the directory for .txt files, sorts them, creates the job list,
// starts worker threads, waits for them, and finally writes everything
// to "text.tzip" in the same order as the sorted file names.
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

            // grow array of file names
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
    if (num_threads > (MAX_THREADS - 1))
        num_threads = MAX_THREADS - 1;
    if (num_threads < 1)
        num_threads = 1;

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
        // first write compressed size (int), then the actual compressed bytes
        fwrite(&g_jobs[i].compressed_size, sizeof(int), 1, f_out);
        fwrite(g_jobs[i].compressed, 1, g_jobs[i].compressed_size, f_out);

        total_in  += g_jobs[i].original_size;
        total_out += g_jobs[i].compressed_size;
    }

    fclose(f_out);

    // print compression statistics
    if (total_in > 0)
        printf("Compression rate: %.2lf%%\n",
               100.0 * (total_in - total_out) / total_in);
    else
        printf("Compression rate: 0.00%%\n");

    // cleanup
    for (int i = 0; i < nfiles; i++) {
        free(g_jobs[i].full_path);
        free(g_jobs[i].compressed);
    }
    free(g_jobs);

    for (int i = 0; i < nfiles; i++)
        free(files[i]);
    free(files);

    return 0;
}
