INCLUDE := # Your Java include path goes here

libthreadaffinity.so: ThreadAffinity.c ThreadAffinity.class
	javah -o ThreadAffinity.h ThreadAffinity
	gcc -I$(INCLUDE) -I$(INCLUDE)/linux $< -lpthread -shared -fPIC -o $@

numa: numa.c
	gcc numa.c -o numa

write: write.c
	gcc write.c -o write

clean:
	rm -rf *.so
