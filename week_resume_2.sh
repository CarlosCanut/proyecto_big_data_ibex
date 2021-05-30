
unique_stonks=$(cat temp/2021_05_22.csv | awk -F"," '{ a[$1]++ } END { for (b in a) { print b } }')
stonks=$(cat temp/2021_05_22.csv | awk -F"," '{print $1}')


for unique_stonk in $unique_stonks; do
	counter=0
	current_stonk=$unique_stonk
	echo currentStonk: $current_stonk
	for stonk in $stonks; do
		if [ $stonk == $current_stonk ]; then
			let counter=counter+1
		fi
	done
	echo $counter
done
