#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <setjmp.h>
#include <signal.h>
#include <gmp.h>
#include <stdint.h>
#include <uv.h>
#include <sigsegv.h>
#include <curses.h>
#include <termios.h>
#include <term.h>
#include <dirent.h>
#include <pmmintrin.h>
#include <xmmintrin.h>

#include "all.h"
#include "v/vere.h"
#include "v/kafk.h"
#include "v/egzh.h"

// stock vere
void _sist_home(u2_reck* rec_u);
void _sist_zest(u2_reck* rec_u);
void _lo_init();

// test main
void setup_loop();

// kafka
void test_kafka_logging_bytes();
void test_kafka_logging_ova_w();
void test_kafka_logging_ova_r(uv_timer_t* handle);
void test_kafka_logging_ova_setup();


// clog
void test_clog_unclog_setup();

// egzh
void test_egz_queue_setup();
void test_egz_bytes_setup();
void test_egz_ova_setup();
void  test_egz_stress();




