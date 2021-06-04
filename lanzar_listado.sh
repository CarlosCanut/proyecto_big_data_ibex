
while getopts ":a:o:" opt; do
  case $opt in
    a) script_name="$OPTARG"
    ;;
    o) output_path="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done


python $script_name -r hadoop hdfs:///stonks/*.csv --output-dir $output_path

