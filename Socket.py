import socket

def main():
	udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

	dest_ip = input("Please input dest_ip:")

	dest_port = int(input("Please input dest_port"))	

	send_data = input("please input sending data: ")

	udp_socket.sendto(send_data.encode("utf-8"), (dest_ip, dest_port))

	recv_data = udp_socket.recvfrom(1024)

	print(recv_data)

	udp_socket.close()

if __name__ == "__main__":
	main()