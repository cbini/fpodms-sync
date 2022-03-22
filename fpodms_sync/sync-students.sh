cd ~/datarobot/datagun
./bin/qgtunnel ~/.pyenv/versions/datagun/bin/python ./datagun/extract.py -C ./datagun/config/fpodms.json

cd ~/datarobot/fpodms
~/.pyenv/versions/fpodms-sync/bin/python ./fpodms_sync/sync-students.py
