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
        os.system("scp /arth-task/task7.1/dn/hdfs-site.xml {}:/etc/hadoop/hdfs-site.xml".format(ip))
        os.system("rm -f /arth-task/task7.1/dn/hdfs-site.xml")

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
        os.system("scp /arth-task/task7.1/dn/core-site.xml {}:/etc/hadoop/core-site.xml".format(ip))
        os.system("rm -f /arth-task/task7.1/dn/core-site.xml")

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
        os.system('scp /root/jdk-8u171-linux-x64.rpm /root/hadoop-1.2.1-1.x86_64.rpm {}:/root/'.format(ip))
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
        os.system('scp /root/jdk-8u171-linux-x64.rpm /root/hadoop-1.2.1-1.x86_64.rpm {}:/root/'.format(ip))
        os.system('ssh {} rpm -ivh /root/jdk-8u171-linux-x64.rpm'.format(ip))
        os.system('ssh {} rpm -ivh /root/hadoop-1.2.1-1.x86_64.rpm --force'.format(ip))
        hdfs_site("data",location)
        core_site("data",location)
        os.system('ssh {} hadoop-daemon.sh start datanode'.format(ip))
        os.system('ssh {} jps'.format(ip))

def hadoop_lvm(location):
    if location==1:
                #creating lvm partition 
               dev = input("Enter Device Name : ")
               os.system('pvcreate {}'.format(dev))
               print("Created pv : {}".format(dev))
               os.system('pvdisplay {}'.format(dev))
               vg = input("Enter Name of Volume Group : ")
               os.system('vgcreate {}  {}'.format(vg , dev))
               os.system('vgdisplay {}'.format(vg))
               lv = input("Enter Logical Volume Name : ")
               sz = input("Enter Size of Partition you want : ")
               os.system('lvcreate --size {} --name {} {}'.format(sz , lv ,vg))
               os.system("lvdisplay {}/{}".format(vg ,lv))
               os.system('mkfs.ext4 /dev/{}/{}'.format(vg , lv))
               print("Logical Volume Formatted ..")
               #mounting the lvm partition
               fold = input("Enter the folder Name which you want to be mount on LVM Parition : ")
               vg = input("Enter Volume Group Name : ")
               lv = input("Enter Logical Volume Name : ")
               os.system("mount /dev/{}/{}  {}".format(vg , lv , fold))
               print("Mounted the LVM Partiton ..")
               os.system('df -hT')

    elif location==2:
               #creating lvm partition 
               dev = input("Enter Device Name : ")
               os.system('ssh {} pvcreate {}'.format(ip , dev))
               print("Created pv : {}".format(dev))
               os.system('ssh {} pvdisplay {}'.format(ip , dev))
               vg = input("Enter Name of Volume Group : ")
               os.system('ssh {} vgcreate {}  {}'.format(ip , vg , dev))
               os.system('ssh {} vgdisplay {}'.format(ip , vg))
               lv = input("Enter Logical Volume Name : ")
               sz = input("Enter Size of Partition you want : ")
               os.system('ssh {} lvcreate --size {} --name {} {}'.format(ip , sz , lv ,vg))
               os.system("ssh {} lvdisplay {}/{}".format(ip , vg ,lv))
               os.system('ssh {} mkfs.ext4 /dev/{}/{}'.format(ip ,vg , lv))
               print("Logical Volume Formatted ..")
               #mounting the lvm partition
               folder = input("Enter the folder Name which you want to be mount on LVM Parition : ")
               vg = input("Enter Volume Group Name : ")
               lv = input("Enter Logical Volume Name : ")
               os.system("ssh {} mount /dev/{}/{}  {}".format(ip , vg , lv , folder))
               print("Mounted the LVM Partiton ..")
               os.system('ssh {} df -hT'.format(ip))

def Linux_cmd(location):
    if location==1:
        #Run Cmd on local host
        cmd=input("Enter the command: ")
        os.system(cmd)
    elif location==2:
        #run cmd on remote host
        cmd=input("Enter your command: ")
        os.system("ssh {} {}".format(ip,cmd))

os.system("tput setaf 3")
print("\t\t\t***Welcome To My Program***")
os.system("tput setaf 7")
print("\t\t\t---------------------------\n\n")

Password=input("Enter the password: ")
for i in range(3):
    Password=gp.getpass("Enter the password: ")
    if Password=="redhat":
        print("""
        Where do you want to run your :
        Press 1: for local system
        Press 2: for remote system
        """)
        location=int(input("-->"))
        while True:
            if location==1:
                print("""
                Press 1: Run Basic Linux command
                Press 2: To configure and start the NameNode
                Press 3: To configure and start datanode
                Press 4: To create and mount LVM Partition
                Press 5: To Exit
                """)
                requirement=int(input("-->"))
                if requirement==1:
                    Linux_cmd(location)
                elif requirement==2:
                    namenode(location)
                elif requirement==3:
                    datanode(location)
                 elif requirement==4:
                    datanode(location)
                elif requirement==5:
                    exit()
            elif location==2:
                ip = input("\n Please enter the remote host IP: ")
                print("""\n
                Press 1: Run Basic Linux command
                Press 2: To configure and start the NameNode
                Press 3: To configure and start datanode
                Press 4: To create and mount LVM Partition
                Press 5: To Exit
                """)
                requirement=int(input("-->"))
                if requirement==1:
                    Linux_cmd(location)
                elif requirement==2:
                    namenode(location)
                elif requirement==3:
                    datanode(location)
                elif requirement==4:
                    datanode(location)
                elif requirement==5:
                    exit()
            else:
                print("Invalid input")
    else:
        print("Try Again")
        if i!=0:
            print("you have",3-i,"Chances left")
