# include "stdio.h"
# include "stdarg.h"

int error(int status, int error, char* format, ...)
{
    va_list ap;
    va_start(ap,format);
    vprintf(format,ap);
    va_end(ap);
    return 0;
}

int main()
{
    error(1,1,"%d\n",1);
    error(1,1,"\n");
    error(1,1,"%d %d %d \n",1,2,3);
    return 0;
}