while [ 1 = 1 ]; do 
	git add --all; 
	git commit -m "Scripted auto-commit on change ($(date +"%F %T")) by gitwatch.sh" >/dev/null; 
	git push; 
	sleep 2; 
done