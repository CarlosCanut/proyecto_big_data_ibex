start-dfs.sh

local_stonks=$(ls stonks/)

for stonk in $local_stonks; do
	if hdfs dfs -test -e /stonks/$stonk
	then
		echo already exists	
	else
		hdfs dfs -put stonks/$stonk /stonks/$stonk
		echo file added
	fi
done

