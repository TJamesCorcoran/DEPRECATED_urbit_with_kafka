void util_end_test();
void util_read_gate(uv_timer_t* handle, int status);
void util_run_inside_loop(void (*func_ptr)(), void * data);
void util_run_after_timer(void (*func_ptr)(uv_timer_t* handle, int status), void * data, c3_d first_d, c3_d thereafter_d);
void util_run_loop();
void setup_loop();