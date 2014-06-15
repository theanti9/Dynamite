import socket
import struct

def main():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(("localhost", 45000))
	print "Connected! Bucket: test"
	while True:
		try:
			command = raw_input("> ")
			value = struct.pack("!i%ds" % len(command), len(command), command)
			print "Sending %d" % len(value)
			s.send(value)
			response = s.recv(1024)
			print response
		except KeyboardInterrupt:
			break
	s.close()

if __name__ == '__main__':
	main()