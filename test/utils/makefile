all:
	gcc test_list.c -std=gnu99 -g -o test_list.exe -I ../../utils/ -L ../../utils/ -lutils
	gcc test_decimal.c -std=gnu99 -g -o test_decimal.exe -I ../../utils/ -L ../../utils/ -lutils -lmpdec -ljansson
	gcc test_http.c -std=gnu99 -g -o test_http.exe -I ../../utils/ -I ../../network/ -L ../../utils/ -lutils -L ../../network/ -lnetwork -lm
	gcc test_skiplist.c -std=gnu99 -g -o test_skiplist.exe -I ../../utils/ -L ../../utils/ -lutils
	gcc -g test_zlib.c -std=gnu99 -o test_zlib.exe -I ../../utils/ -L ../../utils/ -lutils -Wl,-Bstatic -lz -fPIC -Wl,-Bdynamic -ldl -lm -Wall -Wno-strict-aliasing -Wno-uninitialized -g -rdynamic
	gcc -g test_zlib2.c -std=gnu99 -o test_zlib2.exe -I ../../utils/ -L ../../utils/ -lutils -Wl,-Bstatic -lz -fPIC -Wl,-Bdynamic -ldl -lm -Wall -Wno-strict-aliasing -Wno-uninitialized -g -rdynamic
	gcc -g test_zlib3.c -std=gnu99 -o test_zlib3.exe -I ../../utils/ -L ../../utils/ -lutils -Wl,-Bstatic -lz -fPIC -Wl,-Bdynamic -ldl -lm -Wall -Wno-strict-aliasing -Wno-uninitialized -g -rdynamic
	gcc -g test_zlib4.c -std=gnu99 -o test_zlib4.exe -I ../../utils/ -L ../../utils/ -lutils -Wl,-Bstatic -lz -fPIC -Wl,-Bdynamic -ldl -lm -Wall -Wno-strict-aliasing -Wno-uninitialized -g -rdynamic
	gcc test_redis1.c -std=gnu99 -g -o test_redis1.exe -I ../../utils/ -L ../../utils/ -lutils -I ../../network/ -L ../../network/ -lnetwork -lm -lhiredis
	gcc test_redis2.c -std=gnu99 -g -o test_redis2.exe -I ../../utils/ -L ../../utils/ -lutils -I ../../network/ -L ../../network/ -lnetwork -lm -lhiredis
	#gcc test_dict.c -std=gnu99 -g -o test_dict.exe -I ../../utils/ -L ../../utils/ -L ../../utils -lutils -L ../../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lmysqlclient -lrdkafka -lhiredis -lcurl -lssl -lcrypto -lz -Wl,-Bdynamic -lm -lpthread -ldl -lstdc++
	#gcc test_queue.c -std=gnu99 -g -o test_queue.exe -I ../../utils/ -I ../../network/ -L ../../utils/ -L ../../utils -lutils -L ../../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lmysqlclient -lrdkafka -lhiredis -lcurl -lssl -lcrypto -lz -Wl,-Bdynamic -lm -lpthread -ldl -lstdc++
	#gcc test_json.c -std=gnu99 -g -o test_json.exe -I ../../utils/ -I ../../network/ -L ../../utils/ -L ../../utils -lutils -L ../../network -lnetwork -Wl,-Bstatic -lev -ljansson -lmpdec -lmysqlclient -lrdkafka -lhiredis -lcurl -lssl -lcrypto -lz -Wl,-Bdynamic -lm -lpthread -ldl -lstdc++
clean:
	rm -f test_list.exe
	rm -f test_decimal.exe
	rm -f test_http.exe
	rm -f test_skiplist.exe
	rm -f test_queue.exe
	rm -f test_dict.exe
