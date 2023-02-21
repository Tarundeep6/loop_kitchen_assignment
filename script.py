import os
file = open("requirements.txt")
while file:
	line = file.readline()
	if line == "":
		break
	os.system("pip3 install " + line)
	# print(line)
file.close()