cd ~/datarobot/datagun
./bin/qgtunnel ~/.pyenv/versions/datagun/bin/python ./datagun/extract.py -C ./datagun/config/fpodms.json

cd ~/datarobot/fpodms
~/.virtualenvs/fpodms/bin/python sync-students.py
