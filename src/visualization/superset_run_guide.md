Superset - Quick Run Guide (WSL)

1. Go to project folder:
cd ~/superset

2. Activate virtualenv:
source venv/bin/activate

3. Run Superset:
superset run -p 8088 --with-threads --reload --debugger

* Open: http://localhost:8088
* Log in
* Update the host address in the database connections settings to the Windows IP address

4. Stop:
Ctrl + C

5. Deactivate:
deactivate