#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include <algorithm> 
#include <time.h> 
#include <pthread.h>
#include <functional>
#include <iostream>
#include <string>
#include <string.h>

#define		SERVER_NO	4

int server_id;
int delay[SERVER_NO];
char IP[SERVER_NO][INET6_ADDRSTRLEN];		//IP addresses of each server specified in configFIle

/*some variable for setting up UDP receive*/
char s_port[50];
int recv_sockfd;
struct addrinfo recv_hints, *recv_servinfo, *recv_p;
int recv_rv;
struct sockaddr_storage recv_their_addr;	socklen_t recv_addr_len;
char recv_s[INET6_ADDRSTRLEN];

typedef struct message_struct{
	int source;			//source server's ID
	bool request;
	bool feedback;

} message;


/*This is a helper function to set up UDP receive*/
int init_recv()
{

	sprintf(s_port, "%d", server_id+3000);
	memset(&recv_hints, 0, sizeof recv_hints);
	recv_hints.ai_family = AF_UNSPEC; // set to AF_INET to force IPv4
	recv_hints.ai_socktype = SOCK_DGRAM;
	recv_hints.ai_flags = AI_PASSIVE; // use my IP
    
	if ((recv_rv = getaddrinfo(NULL, s_port, &recv_hints, &recv_servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(recv_rv));
		return 1;
	}
    
	// loop through all the results and bind to the first we can
	for(recv_p = recv_servinfo; recv_p != NULL; recv_p = recv_p->ai_next) {
		if ((recv_sockfd = socket(recv_p->ai_family, recv_p->ai_socktype,
                             recv_p->ai_protocol)) == -1) {
			perror("listener: socket");
			continue;
		}
        
		if (bind(recv_sockfd, recv_p->ai_addr, recv_p->ai_addrlen) == -1) {
			close(recv_sockfd);
			perror("listener: bind");
			continue;
		}
        
		break;
	}
    
	if (recv_p == NULL) {
		fprintf(stderr, "listener: failed to bind socket\n");
		return 2;
	}
    
	freeaddrinfo(recv_servinfo);
    
	//printf("listener: waiting to recvfrom...\n");
	fcntl(recv_sockfd, F_SETFL, fcntl(recv_sockfd, F_GETFL) | O_NONBLOCK);
	return 0;
}

void server_send(char* destination_IP, int destination_ID, message msg){
	srand(time(NULL));
	usleep((rand()%99)/50*delay[destination_ID]);				//delay the message
	
	int sockfd;
	struct addrinfo hints, *servinfo, *p;
	int rv;
	int numbytes;
	char destination_port[50];
	sprintf(destination_port, "%d", destination_ID+3000);
	
	
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;
	
	if ((rv = getaddrinfo(destination_IP, destination_port, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return;
	}
	
	// loop through all the results and make a socket
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
							 p->ai_protocol)) == -1) {
			perror("talker: socket");
			continue;
		}
		
		break;
	}
	
	if (p == NULL) {
		fprintf(stderr, "talker: failed to bind socket\n");
		return;
	}
	

	if((numbytes = sendto(sockfd, &msg, sizeof(msg), 0, p->ai_addr, p->ai_addrlen)) == -1){
		perror("talker: sendto");
		exit(1);
	}
	
	freeaddrinfo(servinfo);

	close(sockfd);	
}

void delete_key(int key){

}

void get_key(int key, int level){

}

void insert_key(int key, int value, int level){

}

void update_key(int key, int value, int level){

}

void show_all(){

}

void search_key(int key){

}

int server_receive(message* msg){
	return recvfrom(recv_sockfd, msg, sizeof(*msg), 0, (struct sockaddr *)&recv_their_addr, &recv_addr_len);
}

void* server_accept(void *identifier){
	while(1){
		message msg;
		server_receive(&msg);
	}
}


int main(int argc, char *argv[]){
	if (argc != 2) {
	    fprintf(stderr,"usage: ./server config.txt\n");
	    exit(1);
	}
	
	FILE * file = fopen (argv[1],"r");
	if(file == NULL){
		perror("Error opening configuration file");
		return -1;
	}
	
	fscanf(file,"%d", &server_id);
	fscanf(file,"%d", &delay[0]);
	fscanf(file,"%d", &delay[1]);
	fscanf(file,"%d", &delay[2]);
	fscanf(file,"%d", &delay[3]);
	for(int i = 0; i < SERVER_NO; i++){
		if(fscanf(file,"%s", IP[i]) == EOF){
			break;
		}
		printf("IP: %s\n", IP[i]);
	}
	fclose(file);

	printf("I'm server %d\n", server_id);
	for(int i = 0; i < SERVER_NO; i++){
		if(i != server_id) printf("delayTime to server %d is %d\n", i, delay[i]);
	}
	init_recv();
	
	pthread_t t;
	pthread_create(&t, NULL, &server_accept, NULL);


	
	while(1){
		std::string op;
		int op1, op2, op3;
		scanf("%s %i %i %i",op.c_str(), &op1, &op2, &op3);
		std::cout << "we get "<< op<< op1<< op2<< op3<<'\n';
		if(op.compare("delete") == 0){
			delete_key(op1);
		}else if(op.compare("get") == 0){
			get_key(op1, op2);
		}else if(op.compare("insert") == 0){
			insert_key(op1, op2, op3);
		}else if(op.compare("update") == 0){
			update_key(op1, op2, op3);
		}else if(op.compare("show-all") == 0){
			show_all();
		}else if(op.compare("search") == 0){
			search_key(op1);
		}else{
			printf("wrong operation\n");
		}
	}

}
