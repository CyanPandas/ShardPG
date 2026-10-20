# 用法：disk_busy.sh（lab 上拷成 /tmp/yyt-busy.sh） <挂载点>  打印该挂载点所在块设备 10 s 内的繁忙度（/proc/diskstats 第 13 列 io_ticks）
d=$(basename $(readlink -f $(findmnt -no SOURCE "$1")))
a=$(awk -v d=$d '$3==d{print $13}' /proc/diskstats); sleep 10; b=$(awk -v d=$d '$3==d{print $13}' /proc/diskstats)
echo "$d $(( (b-a)/100 ))%"
