#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

typedef struct hdtest {
	
	int tsize; /* total disk bytes to test */
	
	int nfiles;
	
	char fname[30];
	
	int fsize; /* bytes per file */
	int buffsize; /* bytes per write/read operation */
	char * wbuff;
	char * rbuff;
	int nbuffs;
	char fill;

} hdtest;

hdtest * hdtest_alloc(int tsize, int fsize, int buffsize) {
	
	int i;
	
	hdt = (hdtest *) malloc(sizeof(hdtest));
	
	hdt->fill = 170;
	
	hdt->tsize = tsize;
	hdt->fsize = fsize;
	hdt->buffsize = buffsize;
	
	hdt->wbuff = (char *) malloc(sizeof(char)*buffsize);
	hdt->rbuff = (char *) malloc(sizeof(char)*buffsize);
	
	for(i=0; i < buffsize; i++){
		hdt->wbuff[i] = hdt->fill;
	}
	
	for(i=0; i < 30; i++){
		hdt->fname[i] = '_';
	}
	
	hdt->nfiles = tsize/fsize;
	hdt->nbuffs = fsize/buffsize;
	
}

void hdtest_free(hdtest * hdt) {
	free(hdt->wbuff);
	free(hdt->rbuff);
}

int hdtest_file(hdtest * hdt, int i, int * errs, int * tw, int * tr){
	int i;
	int j;
	int t;
	FILE * f;
	
	sprintf(hdt->fname, "testfile_%i_",i);
	f = fopen(fname,"w+");
	
	if (f==NULL){
		return 1;
	}
	
	
	t = time(NULL);
	
	for(i=0; i<nbuffs; i++){
		fwrite(hdt->wbuff,1,hdt->buffsize,f);
	}
	

	
	*tw += time(NULL) - t;
	
	if (ferror(f)){
		fclose(f);
		return ferror(f);
	}
	
	
	fseek(f,0,SEEK_SET);
	
	for(i=0; i<nbuffs; i++){
		
		t = time(NULL);
		
		fread(hdt->rbuff,1,hdt->buffsize,f);
		
		*tr += time(NULL) - t;
		
		for(j=0; j<hdt->buffsize; j++){
			if (hdt->rbuff[j] != hdt->fill){
				*errs += 1;
			}	
		}	
	}
	
	if (ferror(f)){
		fclose(f);
		return ferror(f);
	}
	
	
	fclose(f);
	return 0;
	
}
