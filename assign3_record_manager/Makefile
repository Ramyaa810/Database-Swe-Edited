recordmanager: test_assign3_1.o record_mgr.o buffer_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o rm_serializer.o expr.o
	gcc -o recordmanager test_assign3_1.o record_mgr.o buffer_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o rm_serializer.o expr.o
test_assign2_1.o: test_assign3_1.c
	gcc -c -g test_assign3_1.c
record_mgr.o: record_mgr.c
	gcc -c -g record_mgr.c
buffer_mgr.o: buffer_mgr.c
	gcc -c -g buffer_mgr.c
storage_mgr.o: storage_mgr.c
	gcc -c -g storage_mgr.c
rm_serializer.o: rm_serializer.c
	gcc -c -g rm_serializer.c
dberror.o: dberror.c
	gcc -c -g dberror.c
expr.o: expr.c
	gcc -c -g expr.c
buffer_mgr_stat.o: buffer_mgr_stat.c
	gcc -c -g buffer_mgr_stat.c
run: recordmanager
	./recordmanager
clean:
	rm recordmanager test_assign3_1.o record_mgr.o buffer_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o