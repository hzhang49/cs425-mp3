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

int server_id;				//the server id
int delay[SERVER_NO];		//this server's delay time to other servers
char IP[SERVER_NO][INET6_ADDRSTRLEN];		//IP addresses of each server specified in configFIle

/*some variable for setting up UDP receive*/
char s_port[50];
int recv_sockfd;
struct addrinfo recv_hints, *recv_servinfo, *recv_p;
int recv_rv;
struct sockaddr_storage recv_their_addr;	socklen_t recv_addr_len;
char recv_s[INET6_ADDRSTRLEN];

//the message structure used to send message to other servers
typedef struct message_struct{

	bool request;		// set request to true if sending request to other servers
	bool feedback;		// set true if sending feedback to other servers
	bool success;
	char request_type[10];		//request type, can be"get", "insert", "delete", "update", "show-all", or "search"
	int source;			//source server's ID
	int key;				
	int value;
	time_t timestamp;

} message;

//value structure to put into our key_value map
typedef struct value_truct{
	int value;
	time_t timestamp;
	int source;
}val;
map<int,val> key_value;		//key_value map

//structure to store value used in get function
typedef struct get_truct{
	int value;
	time_t timestamp;
	int level;
}get;
map<int, get> get_map;

//structure to store value used in update function
typedef struct update_truct{
	int level;
	int con_level;
}update;
map<int, update> update_map;


//structure to store value used in search function
typedef struct search_truct{
	bool has[SERVER_NO];
	int value[SERVER_NO];
	int level;
	time_t timestamp[SERVER_NO];
}ser;
map<int, ser> search_map;



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
	//fcntl(recv_sockfd, F_SETFL, fcntl(recv_sockfd, F_GETFL) | O_NONBLOCK);
	return 0;
}

//udp send
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

//function to delete a key's replica from all servers
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


//function to get a key, depend on the consistency level
//if level 1, the server will send requests and print the key-value returned fastest
//if level 9, the server will send requests and print out the key-value after it get all three replicas, 
//and if there's inconsistency, the value with largest timestamp will win
void get_key(int key, int level){
	if(level == 1){
		if(key_value.find(key) != key_value.end()){
			cout<<"found key "<<key<<" with value "<<(key_value.find(key)->second).value << " in level 1\n";
		}else{
			get input;
			input.value = 0;
			input.level = 1;
			input.timestamp = 0;
			get_map.insert(pair<int, get>(key, input));

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

//function to insert a key, depend on the consistency level
//if level 1, the server will just store the key-value pair locally
//if level 9, the server will store key-value pair to server with server_id != key mod 4
void insert_key(int key, int value, int level){

	if(level == 1){
		if(key_value.find(key) == key_value.end()){
			val input;
			input.value = value;
			input.source = server_id;
			input.timestamp = time(0);
			cout<<"inserted key "<< key<<" with value "<< value<<'\n';
			key_value.insert(pair<int, val> (key, input));
		}
	}else{
		message msg;
		msg.request = true;
		msg.source = server_id;
		msg.feedback = false;
		strcpy(msg.request_type,"insert");
		msg.key = key;
		msg.value = value;
		msg.timestamp = time(0);
		
		if(((key%SERVER_NO) == server_id) && (key_value.find(key) == key_value.end())){

			for(int i = 0; i < SERVER_NO; i++){
				if(server_id != i){
					server_send(IP[i], i, msg);
				}
			}
			cout<<"inserted key "<< key<<" with value "<< value<<" at level "<<level<<'\n';
		}else if (key_value.find(key) == key_value.end()){
			val input;
			input.value = value;
			input.source = server_id;
			input.timestamp = msg.timestamp;		
			key_value.insert(pair<int, val> (key, input));

			for(int i = 0; i < SERVER_NO; i++){
				if((server_id != i) && (i != (key%SERVER_NO))){
					server_send(IP[i], i, msg);
				}
			}
			cout<<"inserted key "<< key<<" with value "<< value<<" at level "<<level<<'\n';
		}
	}
}

void update_key(int key, int value, int level){

	if(level == 1){
		
		if(key_value.find(key) != key_value.end()){
			cout<<"updated key "<<key<<" with value "<<value << " at level 1\n";
			key_value.find(key)->second.value = value;
			key_value.find(key)->second.timestamp = time(0);
		}else{
			update input;
			input.level = 3;
			input.con_level = 1;
			update_map.insert(pair<int, update>(key, input));

			message msg;
			msg.source = server_id;
			msg.request = true;
			msg.feedback = false;
			strcpy(msg.request_type,"update");
			msg.key = key;
			msg.value = value;
			msg.timestamp = time(0);
		
			for(int i = 0; i < SERVER_NO; i++){
				if(server_id != i){
					server_send(IP[i], i, msg);
				}
			}
		}
	}else{
		update input;
		input.level = 3;
		input.con_level = 9;
		update_map.insert(pair<int, update>(key, input));

		message msg;
		msg.source = server_id;
		msg.request = true;
		msg.feedback = false;
		strcpy(msg.request_type,"update");
		msg.key = key;
		msg.value = value;
		msg.timestamp = time(0);
	
		for(int i = 0; i < SERVER_NO; i++){
			if(server_id != i){
				server_send(IP[i], i, msg);
			}
		}
	}
}

void show_all(){
	
	for(map<int, val>::iterator it = key_value.begin(); it != key_value.end(); ++it){
		cout<<"key "<<it->first<<" with value "<<it->second.value<<"\n";
	}
	
}

void search_key(int key){
	ser input;
	for(int i = 0; i < SERVER_NO; i++){
		input.has[i] = false;
		input.value[i] = -1;
		input.level = 3;
		//input.timestamp = (time_t)0;
	}
	
	if(key_value.find(key) != key_value.end()){
		input.has[server_id] = true;
		input.value[server_id] = key_value.find(key)->second.value;
	}
	search_map.insert(pair<int, ser>(key, input));

	message msg;
	msg.source = server_id;
	msg.request = true;
	msg.feedback = false;
	strcpy(msg.request_type,"search");
	msg.key = key;

	for(int i = 0; i < SERVER_NO; i++){
		if(server_id != i){
			server_send(IP[i], i, msg);
		}
	}
}

int server_receive(message* msg){
	return recvfrom(recv_sockfd, msg, sizeof(*msg), 0, (struct sockaddr *)&recv_their_addr, &recv_addr_len);
}

void* server_accept(void *identifier){
	while(1){
		message msg;
		if(server_receive(&msg) > 0){
		
			//if the message is a request
			if(msg.request == true){
				string type = msg.request_type;
				//if the request is delete, just delete
				if(type.compare("delete") == 0){
				
					if(key_value.find(msg.key) != key_value.end()){
						key_value.erase(msg.key);
						cout<<"server "<< server_id<<" deleted key "<< msg.key<<"\n";
					}
					
				//if the request is get, return the key-value pair
				}else if(type.compare("get") == 0){
					
					if(key_value.find(msg.key) != key_value.end()){
						message fb;
						fb.request = false;
						fb.source = server_id;
						fb.feedback = true;
						fb.key = msg.key;
						strcpy(fb.request_type,"get");
						fb.value = (key_value.find(msg.key)->second).value;
						fb.timestamp = (key_value.find(msg.key)->second).timestamp;
		
						server_send(IP[msg.source], msg.source, fb);
						
					}
				
				//if the request is insert , just insert the key-value pair 
				}else if(type.compare("insert") == 0){
					if(key_value.find(msg.key) == key_value.end()){
						val input;
						input.value = msg.value;
						input.source = msg.source;
						input.timestamp = msg.timestamp;
						cout<<"inserted key "<< msg.key<<" with value "<< msg.value<<'\n';
						key_value.insert(pair<int, val> (msg.key, input));
					}
					
				}else if(type.compare("update") == 0){
					if(key_value.find(msg.key) != key_value.end()){
						if(key_value.find(msg.key)->second.timestamp < msg.timestamp){
							message fb;
							fb.request = false;
							fb.source = server_id;
							fb.feedback = true;
							fb.success = true;
							fb.key = msg.key;
							fb.value = msg.value;
							strcpy(fb.request_type,"update");
							key_value.find(msg.key)->second.value = msg.value;
							key_value.find(msg.key)->second.timestamp = msg.timestamp;
							server_send(IP[msg.source], msg.source, fb);
						}else{
							message fb;
							fb.request = false;
							fb.source = server_id;
							fb.feedback = true;
							fb.success = false;
							fb.key = msg.key;
							fb.value = msg.value;
							strcpy(fb.request_type,"update");
							server_send(IP[msg.source], msg.source, fb);
						}
					}					
				}else if(type.compare("search") == 0){
					//cout<<"server  "<< server_id <<" got request "<<"\n";
					if(key_value.find(msg.key) != key_value.end()){
						message fb;
						fb.request = false;
						fb.source = server_id;
						fb.feedback = true;
						fb.success = true;
						fb.key = msg.key;
						fb.value = (key_value.find(msg.key)->second).value;
						fb.timestamp = (key_value.find(msg.key)->second).timestamp;
						strcpy(fb.request_type,"search");
						server_send(IP[msg.source], msg.source, fb);
					}else{
						message fb;
						fb.request = false;
						fb.source = server_id;
						fb.feedback = true;
						fb.success = false;
						fb.key = msg.key;
						strcpy(fb.request_type,"search");
						server_send(IP[msg.source], msg.source, fb);
					}
				}
			//if it's a feedback
			}else if(msg.feedback == true){

				string type = msg.request_type;
			
			/*if it's get feedback, and our server is still waiting for that key's replica*/
				if((type.compare("get") == 0) && ((get_map.find(msg.key) != get_map.end()))){
					//cout<<"got search feedback from "<< msg.source <<" with value "<<msg.value<<"\n";
					(get_map.find(msg.key)->second).level--;		//decrement consistency level
					if((get_map.find(msg.key)->second).value == msg.value){
					
						//update timestamp if our key's current timestamp is earlier then the one we get from msg
						if((get_map.find(msg.key)->second).timestamp < msg.timestamp) (get_map.find(msg.key)->second).timestamp = msg.timestamp;
				
					//if our value is different from the one we get from other server, and their value is newer, update ours
					}else if(((get_map.find(msg.key)->second).value != msg.value) && ((get_map.find(msg.key)->second).timestamp < msg.timestamp)){
						(get_map.find(msg.key)->second).value = msg.value;
						(get_map.find(msg.key)->second).timestamp = msg.timestamp;
					
					}
					if((get_map.find(msg.key)->second).level == 0){
						cout<< "found key " << msg.key<< " with value "<<(get_map.find(msg.key)->second).value << "\n";
						get_map.erase(msg.key);
					}
					
				}else if((type.compare("update")) == 0 && (update_map.find(msg.key) != update_map.end())){
					if((msg.success ==  false) && ((update_map.find(msg.key)->second).con_level == 9)){
						cout<<"update failed\n";
						update_map.erase(msg.key);
					
					}else if((msg.success ==  false) && ((update_map.find(msg.key)->second).con_level == 1) && ((update_map.find(msg.key)->second).level == 1)){
						cout<<"update failed\n";
						update_map.erase(msg.key);
					}else if((msg.success ==  false) && ((update_map.find(msg.key)->second).con_level == 1)){
						(update_map.find(msg.key)->second).level--;
					}else if((msg.success ==  true) && ((update_map.find(msg.key)->second).con_level == 1)){
						cout<<"successfully update key "<<msg.key<<" with value "<< msg.value<< " on level 1\n";
						update_map.erase(msg.key);
					}else if((msg.success ==  true) && ((update_map.find(msg.key)->second).con_level == 9)){
						(update_map.find(msg.key)->second).level--;
						//cout<<"decrementing level\n";
						if((update_map.find(msg.key)->second).level == 0){
							cout<<"successfully update key "<<msg.key<<" with value "<< msg.value<< " on level 9\n";
							update_map.erase(msg.key);
						}
					}
					
				}else if((type.compare("search") == 0) && (search_map.find(msg.key) != search_map.end())){
					//cout<<"got search feedback from "<< msg.source <<"with success "<<(int)(msg.success)<<"\n";
					if(msg.success == true){
						(search_map.find(msg.key)->second).has[msg.source] = true;
						(search_map.find(msg.key)->second).value[msg.source] = msg.value;
						(search_map.find(msg.key)->second).timestamp[msg.source] = msg.timestamp;
					}
					(search_map.find(msg.key)->second).level--;
					if(search_map.find(msg.key)->second.level <=0){
						for(int i = 0; i < SERVER_NO; i++){
							if(search_map.find(msg.key)->second.has[i] == true){
								cout<<"server "<<i<<" has key "<<msg.key<<" with value "<<(search_map.find(msg.key)->second).value[i]<<"\n";
							}
						}
						
						int newest_server = 0;
						for(int i = 0; i < SERVER_NO; i++){
							if((search_map.find(msg.key)->second).timestamp[i] > (search_map.find(msg.key)->second).timestamp[newest_server]){					
								newest_server = i;
							}
						}
						for(int i = 0; i < SERVER_NO; i++){
							if((search_map.find(msg.key)->second).value[i] != (search_map.find(msg.key)->second).value[newest_server]){
								message repair;
								repair.source = newest_server;
								repair.request = true;
								repair.feedback = false;
								strcpy(repair.request_type,"update");
								repair.key = msg.key;
								repair.value = (search_map.find(msg.key)->second).value[newest_server];
								repair.timestamp = (search_map.find(msg.key)->second).timestamp[newest_server];
								server_send(IP[i], i, repair);
							}
						}

						search_map.erase(msg.key);
					}
					

				}
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
		char temp[100];
		int op1, op2, op3;
		//scanf("%[^\n]s",temp);
		cin.getline(temp, 100);
		char temp1[20];

		sscanf(temp, "%s %d %d %d", temp1, &op1, &op2, &op3);
		op = temp1;
		//cout<<"get command: "<< op<<"\n";
		//std::cout << "we get "<<temp<<op<<op1<< op2<< op3<<'\n';
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
