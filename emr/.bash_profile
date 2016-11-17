if [ -n "$BASH" ] && [ -f ~/.bashrc ] && [ -r ~/.bashrc ]; then
  source ~/.bashrc
fi

# custom aliases
alias jupyspark_emr='jupyspark_emr.sh'
alias spark_submit='spark_submit.sh'

# Anaconda2
export PATH=/home/hadoop/anaconda/bin:/home/hadoop/anaconda/bin:/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/sbin:/opt/aws/bin

# custom path additions
export PATH="$PATH:$HOME/bin" 
