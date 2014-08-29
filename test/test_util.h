void util_end_test();
void util_gate_read(uv_timer_t* handle);
void util_gate_done(uv_timer_t* handle);
void util_run_inside_loop(void (*func_ptr)(), void * data);
void util_run_after_timer(void (*func_ptr)(uv_timer_t* handle), 
						  void * data, 
						  c3_d first_d, 
						  c3_d thereafter_d);

void util_loop_run();
void util_loop_stop();

void util_kafka_run();
void util_kafka_stop();

extern int write_done;
extern int test_done;
