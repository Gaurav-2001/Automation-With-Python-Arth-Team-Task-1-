import os
import getpass as gp

def hdfs_site(node,location):
    os.system("cp /arth-task/task7.1/dn/temp.xml /arth-task/task7.1/dn/hdfs-site.xml")
    os.system("echo \<configuration\> >> /arth-task/task7.1/dn/hdfs-site.xml")
    os.system("echo \<property\> >> /arth-task/task7.1/dn/hdfs-site.xml")
    os.system("echo \<name\>dfs.{}.dir\</name\> >> /arth-task/task7.1/dn/hdfs-site.xml".format(node))
    folder=input("Enter the {}node directory to be created and configure: ".format(node))
    if location==1:
        os.system("mkdir {}".format(folder))
    elif location==2:
        os.system("ssh {} mkdir {}".format(ip,folder))
    os.system("echo \<value\>{}\</value\> >> /arth-task/task7.1/dn/hdfs-site.xml".format(folder))
    os.system("echo \</property\> >> /arth-task/task7.1/dn/hdfs-site.xml")
    os.system("echo \</configuration\> >> /arth-task/task7.1/dn/hdfs-site.xml")
    if location==1:
        os.system("mv -f /arth-task/task7.1/dn/hdfs-site.xml /etc/hadoop/hdfs-site.xml")
    elif location==2:
        os.system("ssh {} mv -f /arth-task/task7.1/dn/hdfs-site.xml /etc/hadoop/hdfs-site.xml".format(ip))

def core_site(node,location):
    os.system("cp /arth-task/task7.1/dn/temp.xml /arth-task/task7.1/dn/core-site.xml")
    os.system("echo \<configuration\> >> /arth-task/task7.1/dn/core-site.xml")
    os.system("echo \<property\> >> /arth-task/task7.1/dn/core-site.xml")
    os.system("echo \<name\>fs.default.name\</name\> >> /arth-task/task7.1/dn/core-site.xml")
    nnip=input("Enter the ip of the namenode: ")  
    port=input("Enter the port number of hadoop cluster: ")
    os.system("echo \<value\>hdfs://{}:{}\</value\> >> /arth-task/task7.1/dn/core-site.xml".format(nnip,port))
    os.system("echo \</property\> >> /arth-task/task7.1/dn/core-site.xml")
    os.system("echo \</configuration\> >> /arth-task/task7.1/dn/core-site.xml")
    if location==1:
        os.system("mv -f /arth-task/task7.1/dn/core-site.xml /etc/hadoop/core-site.xml")
    elif location==2:
        os.system("ssh {} mv -f /arth-task/task7.1/dn/core-site.xml /etc/hadoop/core-site.xml".format(ip))

def namenode(location):
    if location==1:
        #Configure and start namenode in local host
        os.system('rpm -ivh /root/jdk-8u171-linux-x64.rpm')
        os.system('rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force')
        hdfs_site("name",location)
        core_site("name",location)
        os.system('hadoop namenode -format')
        os.system('hadoop-daemon.sh start namenode')
        os.system('jps')
    elif location==2:
        #Configure and start namenode in remote host
        os.system('ssh {} rpm -ivh /root/jdk-8u171-linux-x64.rpm'.format(ip))
        os.system('ssh {} rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force'.format(ip))
        hdfs_site("name",location)
        core_site("name",location)
        os.system('ssh {} hadoop namenode -format'.format(ip))
        os.system('ssh {} hadoop-daemon.sh start namenode'.format(ip))
        os.system('ssh {} jps'.format(ip))
        



def datanode(location):
    if location==1:
        #Configure and start datanode in local host
        os.system('rpm -ivh /root/jdk-8u171-linux-x64.rpm')
        os.system('rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force')
        hdfs_site("data",location)
        core_site("data",location)
        os.system('hadoop-daemon.sh start datanode')
        os.system('jps')
    elif location==2:
        #Configure and start datanode in remote host
        os.system('ssh {} rpm -ivh /root/jdk-8u171-linux-x64.rpm'.format(ip))
        os.system('ssh {} rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force'.format(ip))
        hdfs_site("data",location)
        core_site("data",location)
        os.system('ssh {} hadoop-daemon.sh start datanode'.format(ip))
        os.system('ssh {} jps'.format(ip))


os.system("tput setaf 3")
print("\t\t\t***Welcome To My Program***")
os.system("tput setaf 7")
print("\t\t\t---------------------------\n\n")

print("""
Where do you want to run your : 
Press 1: for local system 
Press 2: for remote system
""")
location=int(input("-->"))
if location==1:
    print("""
    Press 1: To configure and start the NameNode
    Press 2: To configure and start datanode
    Press 3: To Exit
    """)
    requirement=int(input("-->"))
    if requirement==1:
        namenode(location)
    elif requirement==2:
        datanode(location)
    elif requirement==3:
        exit()
elif location==2:
    ip = input("\n Please enter the remote host IP: ")
    print("""\n
    Press 1: To configure and start the NameNode
    Press 2: To configure and start datanode
    Press 3: To Exit
    """)
    requirement=int(input("-->"))
    if requirement==1:
        namenode(location)
    elif requirement==2:
        datanode(location)
    elif requirement==3:
        exit()
else:
    print("Invalid input")
