start-dfs.sh
git pull

read -p "¿Quieres cargar todos los datos a hdfs o solo actualizar nuevos datos? (total : todos los datos (.csv), new : actualizar nuevos datos (.csv))" primera_ejecucion

if [ $primera_ejecucion == 'total' ]
then
	local_stonks=$(ls -p stonks | grep -v /)
else
	local_stonks=$(git diff --name-only HEAD^1..HEAD | awk -F" " '{split($1,a,"."); if(a[2] == "csv") print($1)}')
fi


for stonk in $local_stonks; do
	echo $stonk
	if [ $primera_ejecucion == 'total' ]
	then
		if hdfs dfs -test -e /stonks/$stonk
		then
			echo already exists
		else
			hdfs dfs -put stonks/$stonk /stonks/$stonk
			echo file added
		fi
	else
		if hdfs dfs -test -e /$stonk
		then
			echo already exists
		else
			hdfs dfs -put $stonk /$stonk
			echo file added
		fi
	fi
done

