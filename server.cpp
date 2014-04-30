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
#include <map>

#define		SERVER_NO	4

using namespace std;

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

	char request_type[10];

	int key;
	int value;
	time_t timestamp;
} message;

typedef struct value_truct{
	int value;
	time_t timestamp;
	int source;
}val;
map<int,val> key_value;

typedef struct get_truct{
	int value;
	int timestamp;
	int level;
}get;
map<int, get> get_map;




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
	key_value.erase(key);
	message msg;
	msg.source = server_id;
	msg.request = true;
	msg.feedback = false;
	strcpy(msg.request_type,"delete");
	msg.key = key;
	for(int i = 0; i < SERVER_NO; i++){
		if(server_id != i){
			server_send(IP[i], i, msg);
		}
	}
}

void get_key(int key, int level){
	if(level = 1){
		if(key_value.find(key) != key_value.end()){
			cout<<"found key "<<key<<" with value "<<(key_value.find(key)->second).value << "in level 1\n";
		}
	}else{
		get input;
		input.value = 0;
		input.level = 3;
		input.timestamp = 0;
		get_map.insert(pair<int, get>(key, input));
		if(key_value.find(key) != key_value.end()){
			(get_map.find(key)->second).value = (key_value.find(key)->second).value;
			(get_map.find(key)->second).level--;
			(get_map.find(key)->second).timestamp = (key_value.find(key)->second).timestamp;
		}
		
		message msg;
		msg.source = server_id;
		msg.request = true;
		msg.feedback = false;
		strcpy(msg.request_type,"get");
		msg.key = key;
		
		
		for(int i = 0; i < SERVER_NO; i++){
			if(server_id != i){
				server_send(IP[i], i, msg);
			}
		}
	}
}

void insert_key(int key, int value, int level){
	if(level = 1){
		if(key_value.find(key) == key_value.end()){
			val input;
			input.value = value;
			input.source = server_id;
			input.timestamp = time(0);
			cout<<"time stamp is "<< input.timestamp<<'\n';
			key_value.insert(pair<int, val> (key, input));
		}
	}else{
		
	}
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
		if(server_receive(&msg) > 0){
			if(msg.request == true){
				string type = msg.request_type;
				if(type.compare("delete") == 0){
				
					key_value.erase(msg.key);
					
				}else if(type.compare("get") == 0){
					
					if(key_value.find(msg.key) != key_value.end()){
						message fb;
						fb.request = false;
						fb.source = server_id;
						fb.feedback = true;
						fb.key = msg.key;
						strcpy(fb.request_type,"get");
						fb.value = (key_value.find(key)->second).value;
						fb.timestamp = (key_value.find(key)->second).timestamp;
						server_send(IP[msg.source], msg.source, fb);
						
					}
					
				}else if(type.compare("insert") == 0){
					
				}else if(type.compare("update") == 0){
					
				}else if(type.compare("show-all") == 0){
					
				}else if(type.compare("search") == 0){
					
				}
			}
		}else if(msg.feedback == true){
			string type = msg.request_type;
			
			/*if it's get feedback, and our server is still waiting for that key's replica*/
			if((type.compare("get") == 0) && (get_map.find(msg.key) != get_map.end())){
					
				if((get_map.find(msg.key)->second).value == msg.value){
					(get_map.find(msg.key)->second).level--;		//decrement consistency level
					//update timestamp if our key's current timestamp is earlier then the one we get from msg
					if((get_map.find(msg.key)->second).timestamp < msg.timestamp) (get_map.find(msg.key)->second).timestamp = msg.timestamp;
				
				//if our value is different from the one we get from other server, and their value is newer, update ours
				}else if(((get_map.find(msg.key)->second).value != msg.value) && ((get_map.find(msg.key)->second).timestamp < msg.timestamp)){
					(get_map.find(msg.key)->second).value = msg.value;
					(get_map.find(msg.key)->second).timestamp = msg.timestamp;
					(get_map.find(msg.key)->second).level--;
				}
				if((get_map.find(msg.key)->second).level == 0){
					cout<< "found key " << msg.key<< "with value "<<(get_map.find(msg.key)->second).value << "in level 3\n";
					get_map.erase(msg.key);
				}
					
			}else if(type.compare("insert") == 0){
					
			}else if(type.compare("update") == 0){
					
			}else if(type.compare("show-all") == 0){
					
			}else if(type.compare("search") == 0){
					
			}
		}
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
		char temp[10];
		int op1, op2, op3;
		scanf("%s %i %i %i",temp, &op1, &op2, &op3);
		op = temp;
		//std::cout << "we get "<< op<< op1<< op2<< op3<<'\n';
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
