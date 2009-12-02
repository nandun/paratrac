#include <string>
#include <stdlib.h>
#include <stdio.h>
using namespace std;


extern "C" {
  int dis(const char* str1, const char* str2);
  int test(const char* str, int a);
}



int test(const char* str, int a){
	printf("%s\n",str);
	return 0;
}

inline int snake(int k, int y, const char* str1, const char* str2, int len1, int len2)
{
  int x = y - k;
  while (x < len1 && y < len2 && str1[x] == str2[y]) {
    x++;
    y++;
  }
  return y;
}

int dis(const char* str1, const char* str2)
{
  int len1 = strlen(str1);
  int len2 = strlen(str2);
  int SIZE = len1+len2+2;
  int* fp = new int[SIZE];
  for (int i = 0; i < SIZE; i++) fp[i] = -1;
  const char* s1=str1;
  const char* s2=str2;
  if(len1>len2){
    s1=str2;
    s2=str1;
    int t = len1;
    len1 = len2;
    len2 = t;
  }
  int x, y, k, p;
  int offset = len1 + 1;
  int delta = len2 - len1;

  for (p = 0; fp[delta + offset] != len2; p++) {
    for(k = -p; k < delta; k++){
      fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), s1, s2, len1, len2);
    }
    for(k = delta + p; k > delta; k--){
      fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), s1, s2, len1, len2);
    }
    fp[delta + offset] = snake(delta, max(fp[delta-1+offset] + 1, fp[delta+1+offset]), s1, s2, len1, len2);
  }

  delete[] fp;

  return delta + (p - 1) * 2;
}



