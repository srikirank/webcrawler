cp /root/spring14/webcrawler/conf/hadoop/* /root/software/hadoop-1.1.2/conf/
cp /root/spring14/webcrawler/conf/hbase/* /root/software/hbase-0.94.7/conf
cp /root/spring14/webcrawler/hosts /etc/

scp /root/software/hadoop-1.1.2/conf/* root@slave1:/root/software/hadoop-1.1.2/conf/
scp /root/software/hadoop-1.1.2/conf/* root@slave2:/root/software/hadoop-1.1.2/conf/
scp /root/software/hadoop-1.1.2/conf/* root@slave3:/root/software/hadoop-1.1.2/conf/

scp /root/spring14/webcrawler/conf/hbase/* root@slave1:/root/software/hbase-0.94.7/conf
scp /root/spring14/webcrawler/conf/hbase/* root@slave2:/root/software/hbase-0.94.7/conf
scp /root/spring14/webcrawler/conf/hbase/* root@slave3:/root/software/hbase-0.94.7/conf
