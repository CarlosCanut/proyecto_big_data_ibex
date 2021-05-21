start-dfs.sh
git pull

local_stonks=$(git diff --name-only HEAD^1..HEAD | awk -F" " '{split($1,a,"."); if(a[2] == "csv") print($1)}')

for stonk in $local_stonks; do
	if hdfs dfs -test -e /stonks/$stonk
	then
		echo already exists	
	else
		hdfs dfs -put stonks/$stonk /stonks/$stonk
		echo file added
	fi
done

