#include <string>
#include <stdlib.h>
#include <stdio.h>

using namespace std;


int snake(int k, int y, const string& str1, const string& str2)
{
  int x = y - k;

  while (x < str1.size() && y < str2.size() && str1[x] == str2[y]) {
    x++;
    y++;
  }

  return y;
}

int edit_distance_onp(const string& str1, const string& str2)
{
  int SIZE = str1.size() + str2.size()+2;
  int* fp = new int[SIZE];
  for (int i = 0; i < SIZE; i++) fp[i] = -1;

  // required: s1->size() <= s2->size()
  const string* const s1 = str1.size() > str2.size() ? &str2 : &str1;
  const string* const s2 = str1.size() > str2.size() ? &str1 : &str2;
  int x, y, k, p;
  int offset = s1->size() + 1;
  int delta = s2->size() - s1->size();


  for (p = 0; fp[delta + offset] != s2->size(); p++) {
    for(k = -p; k < delta; k++)
      fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), *s1, *s2);
    for(k = delta + p; k > delta; k--)
      fp[k + offset] = snake(k, max(fp[k-1+offset] + 1, fp[k+1+offset]), *s1, *s2);
    fp[delta + offset] = snake(delta, max(fp[delta-1+offset] + 1, fp[delta+1+offset]), *s1, *s2);
  }

  delete[] fp;

  return delta + (p - 1) * 2;
}


int main() {
  char* c = new char[7];
  int N = 6;

  while(true){
  for(int i=0;i<7;i++) c[i] = 0;
  for(int i=0;i<N;i++){
     int ft = fgetc(stdin);
     if(ft==EOF){
       return 0;
     }
     char t = (char) ft;
     if(t==' ')break;
     c[i]=t;
  }
  int len = atoi(c);
  for(int i=0;i<7;i++) c[i] = 0;
  for(int i=0;i<N;i++){
     char t = fgetc(stdin);
     if(t==' ')break;
     c[i]=t;
  }
  int len2 = atoi(c);

  char* str1 = new char[len+1];
  for(int i=0;i<len;i++) str1[i] = fgetc(stdin);
  str1[len]=0;
  char* str2 = new char[len2+1];
  for(int i=0;i<len2;i++) str2[i] = fgetc(stdin);
  str2[len2]=0;
  //  printf("%d,=%d,%d,%s,%s\n", edit_distance_onp(*(new string(str1)), *(new string(str2))),len,len2, str1, str2);
  printf("%d\n", edit_distance_onp(*(new string(str1)), *(new string(str2))));
  //printf("%d,%d,%s,%s\n", len,len2, str1, str2);
  //printf("%d\n", 128);
  fflush(stdout);
  delete[] str1;
  delete[] str2;
  }
  return 0;
}
