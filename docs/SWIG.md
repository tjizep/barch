To build swig you need these packages
```bash
sudo apt-get install swig
sudo apt-get install python3-dev
sudo apt-get install pip
```
These may also be required if {barch build dir}/setup.py does not work via `pip install .`
It usually fails because setuptools and pip versions are incompatible
```
python -m pip install --upgrade pip
python -m pip install --upgrade setuptools wheel twine check-wheel-contents
```
kill all dockers
```bash
sudo docker stop $(sudo docker ps -q)
```