// Project 2 serial.h header file
// Group: Anisha Hossain Megha, Alina Pineda Lopez, Brielle Ashmeade, Georgia Ng Wai
// Description: parallel version of the serial compressor using pthreads.


#ifndef SERIAL_H
#define SERIAL_H

#include <stddef.h>

//must stay <= 20 total including main 
#define NUM_WORKER_THREADS 8

//main function called by main.c — compresses files in parallel 
void run_serial(const char *input_dir, const char *output_zip);

#endif /* SERIAL_H */
