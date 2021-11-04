
#include<string.h>
#include "Utils.h"


string Utils::format_string(const char *format,...) {
    va_list args;
    int len;
    char * buffer;

    va_start( args, format );
    len = vsnprintf(nullptr, 0, format, args);
    va_end(args);
    buffer = new char[len * sizeof(char)+1];
    va_start( args, format );
    vsprintf( buffer, format, args );
    va_end(args);
    string s = buffer;
    delete []buffer;
    return s;
}

string Utils::format_string(const char *format,va_list args) {
    va_list cpy;
    va_copy(cpy,args);
    int len;
    char * buffer;
    len = vsnprintf(nullptr, 0, format, args);
    buffer = new char[len * sizeof(char)+1];
    vsprintf( buffer, format, cpy );
    string s = buffer;
    delete []buffer;
    return s;
}

char* Utils::dup_string(const string &s) {
    char *buf = new char[s.length()+1];
    strcpy(buf,s.c_str());
    return buf;
}

int Utils::batch_insert(SQLExecutor *executor,const char* table_name, const char* field_list, CLargeStringArray& value_vec,int exec_count) {
	int result = 0;
	char *sql,*value_list;
	size_t count = value_vec.GetCount();
	size_t i=0;
	while(i<count) {
		char *s = value_vec[i];
		FormatString(&value_list,"(%s)", s);
		unsigned int k=1;
		i++;
		while(i<count&&k<(unsigned int)exec_count) {
			s = value_vec[i];
			StringCatenate(&value_list,",(%s)",s);
			i++;
			k++;
		}
		FormatString(&sql,"insert into %s(%s) values %s;", table_name, field_list, value_list);
		delete []value_list;
		if (executor->exec(sql)) {
			delete []sql;
			result = -1;
			break;
		}
		delete []sql;
	}

	return result;
}

string Utils::mpd_to_string(mpd_t *val) {
    char *str = mpd_to_sci(val, 0);
    string s = str;
    free(str);
    return s;
}


void error(int exit_code, int err_code, const char *fmt,...) {
   	va_list args;
	va_start( args, fmt );
	char *buf = 0;
	Utils::FormatString(&buf,fmt,args);
	fprintf(stdout,"%s(exit code: %d,errno:%d)",buf,exit_code,err_code);
	exit(exit_code);
}

void Utils::FormatString(string &s,const char *format,va_list args) {
    va_list args_copy;
    va_copy(args_copy,args);
    unsigned int len = vsnprintf(nullptr,0,format,args_copy);

    char *buffer  = new char[len * sizeof(char)+1];
    vsprintf( buffer, format, args );
	s = buffer;
	delete []buffer;
}

int Utils::FormatString(char **ppbuf,const char *format,...)
{
	va_list args;
	va_start( args, format );
	return FormatString(ppbuf,format,args);
}

int Utils::FormatString(char **ppbuf,const char *format,va_list args) {
    va_list args_copy;
    va_copy(args_copy,args);
    unsigned int len = vsnprintf(nullptr,0,format,args_copy);

    char *buffer  = new char[len * sizeof(char)+1];
    vsprintf( buffer, format, args );

    *ppbuf = buffer;

	return len;
}


int Utils::StringCatenate(char **ppbuf,const char *format,...) {
	char *buffer = 0;
	va_list args;
	va_start( args, format );
	int len = FormatString(&buffer,format,args);
	len = AppendString(ppbuf,buffer);
	delete []buffer;

	return len;
}


int Utils::AppendString(char **ppbuf,char *s) {
	int len1 = (int)strlen(*ppbuf);
	int len2 = (int)strlen(s);
	int len = len1+len2+1;
	char *buffer = new char[len];
	strncpy(buffer,*ppbuf,len);
	strcat(buffer,s);

	delete []*ppbuf;
	*ppbuf = buffer;

	return len;
}



