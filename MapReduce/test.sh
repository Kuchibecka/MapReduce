sh generateData.sh 100
res=$?
    while [ "$i" != "5" ]
      do
	echo $res
        if [[ $(($res)) -eq 1 ]]; then
          i=$(("5"))
          echo "data generated"
        fi
        sleep 3
        i=$(( $i + 1 ))
      done
