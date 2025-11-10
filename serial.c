// Project 2 - Parallel Text Compression
// Authors: Alina Pineda, Anisha Hossain, Brielle Ashmeade, Georgia Ng Wai
// Description:
// This program speeds up text compression by using POSIX threads
// to compress multiple ".txt" files in parallel. Each thread
// reads, compresses, and stores the data for one or more files,
// while the main thread merges the results into a single output
// file "text.tzip" in lexicographical order.

// Constraints:
// - Only pthread library used (no other concurrency APIs)
// - Max 20 threads total (main + workers)
// - Output format matches the baseline serial implementation

#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576      // 1MB buffer size for I/O and compression
#define MAX_THREADS 20           // Max threads allowed (including main)

// ---------- sorting helper ----------
// Compare function for qsort to sort filenames lexicographically
static int cmp(const void *a, const void *b) {
    return strcmp(*(char * const *) a, *(char * const *) b);
}

// ---------- job structure ----------
// Represents a single compression task for one text file
typedef struct {
    char  *full_path;            // Full file path (directory + filename)
    unsigned char *compressed;   // Pointer to compressed data buffer
    int compressed_size;         // Size of compressed data in bytes
    int original_size;           // Size of uncompressed data in bytes
    int done;                    // Flag indicating if compression is complete
} file_job_t;

// ---------- global shared variables ----------
// Shared job array and counters accessible by all threads
static file_job_t *g_jobs = NULL;
static int g_nfiles = 0;
static pthread_mutex_t g_job_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_next_job = 0;       // Index of next file to process (shared counter)

// Worker thread: Compresses files concurrently
// Each thread repeatedly grabs the next available file index,
// reads the file, compresses it, and stores the result.
static void *worker_thread(void *arg) {
    (void)arg;

    // Allocate scratch buffers ONCE per thread to reduce malloc/free overhead
    unsigned char *buffer_in  = malloc(BUFFER_SIZE);
    unsigned char *buffer_out = malloc(BUFFER_SIZE);
    assert(buffer_in && buffer_out);

    for (;;) {
        // --- Fetch the next available file index safely ---
        pthread_mutex_lock(&g_job_lock);
        int idx = g_next_job++;
        pthread_mutex_unlock(&g_job_lock);

        // If no more files remain, exit loop
        if (idx >= g_nfiles)
            break;

        file_job_t *job = &g_jobs[idx];

        // --- Read file into input buffer ---
        FILE *f_in = fopen(job->full_path, "rb");
        assert(f_in != NULL);
        int nbytes = (int)fread(buffer_in, 1, BUFFER_SIZE, f_in);
        fclose(f_in);
        job->original_size = nbytes;

        // --- Compress the file using zlib ---
        z_stream strm;
        int ret = deflateInit(&strm, 9);  // Level 9 = max compression
        assert(ret == Z_OK);

        strm.avail_in  = nbytes;
        strm.next_in   = buffer_in;
        strm.avail_out = BUFFER_SIZE;
        strm.next_out  = buffer_out;

        ret = deflate(&strm, Z_FINISH);
        assert(ret == Z_STREAM_END);

        int nbytes_zipped = (int)(BUFFER_SIZE - strm.avail_out);
        deflateEnd(&strm);

        // --- Store result in job struct ---
        unsigned char *out_copy = malloc(nbytes_zipped);
        assert(out_copy != NULL);
        memcpy(out_copy, buffer_out, nbytes_zipped);

        job->compressed      = out_copy;
        job->compressed_size = nbytes_zipped;
        job->done            = 1;
    }

    // Free per-thread buffers before exit
    free(buffer_in);
    free(buffer_out);
    return NULL;
}

// Main entry point: compress_directory()
// Scans the directory, creates worker threads to compress files,
// and writes all results into a single archive in order.
int compress_directory(char *directory_name) {
    DIR *d;
    struct dirent *dir;
    char **files = NULL;
    int nfiles = 0;

    // --- Open directory ---
    d = opendir(directory_name);
    if (d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // --- Collect all .txt files ---
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

    // --- Sort files lexicographically (required by spec) ---
    qsort(files, nfiles, sizeof(char *), cmp);

    // --- Initialize job list ---
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

    // --- Decide how many threads to create ---
    int num_threads = nfiles;
    if (num_threads > (MAX_THREADS - 1))    // 1 slot reserved for main
        num_threads = MAX_THREADS - 1;
    if (num_threads < 1)
        num_threads = 1;

    pthread_t threads[MAX_THREADS];

    // --- Create worker threads ---
    for (int i = 0; i < num_threads; i++) {
        int rc = pthread_create(&threads[i], NULL, worker_thread, NULL);
        assert(rc == 0);
    }

    // --- Wait for all workers to finish ---
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // --- Write compressed data in order to output file ---
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

    // --- Report compression performance ---
    if (total_in > 0)
        printf("Compression rate: %.2lf%%\n",
               100.0 * (total_in - total_out) / total_in);
    else
        printf("Compression rate: 0.00%%\n");

    // --- Free all allocated memory ---
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
