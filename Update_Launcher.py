import os
import urllib.request
import zipfile
import shutil
import time


slicetrackgeni_master_directory = os.getcwd()
slicetrackgeni_file = slicetrackgeni_master_directory+"\\SliceTrackGeni.exe"
Old_slicetrackgeni_directory = slicetrackgeni_master_directory+"\\slicetrackgeni_exe-master"

proxy_handler = urllib.request.ProxyHandler({'https': 'http://proxy-dmz.intel.com:912'})
opener = urllib.request.build_opener(proxy_handler)
urllib.request.install_opener(opener)


def installation():
    urllib.request.urlretrieve("https://github.com/idriss-animashaun-intel/slicetrackgeni/archive/refs/heads/master.zip", slicetrackgeni_master_directory+"\\slicetrackgeni_luancher_new.zip")
    print("*** Updating Launcher Please Wait ***")
    zip_ref = zipfile.ZipFile(slicetrackgeni_master_directory+"\\slicetrackgeni_luancher_new.zip", 'r')
    zip_ref.extractall(slicetrackgeni_master_directory)
    zip_ref.close()
    os.remove(slicetrackgeni_master_directory+"\\slicetrackgeni_luancher_new.zip")

    src_dir = slicetrackgeni_master_directory + "\\slicetrackgeni-master"
    dest_dir = slicetrackgeni_master_directory
    fn = os.path.join(src_dir, "SliceTrackGeni.exe")
    shutil.copy(fn, dest_dir)

    shutil.rmtree(slicetrackgeni_master_directory+"\\slicetrackgeni-master")

    time.sleep(5)
    
def upgrade():
    print("*** Updating Launcher Please Wait ***")    
    print("*** Removing old files ***")
    time.sleep(20)
    os.remove(slicetrackgeni_file)
    time.sleep(10)
    installation()


### Is slicetrackgeni already installed? If yes get file size to compare for upgrade
if os.path.isfile(slicetrackgeni_file):
    local_file_size = int(os.path.getsize(slicetrackgeni_file))
    # print(local_file_size)

    url = 'https://github.com/idriss-animashaun-intel/slicetrackgeni/raw/master/SliceTrackGeni.exe'
    f = urllib.request.urlopen(url)

    i = f.info()
    web_file_size = int(i["Content-Length"])
    # print(web_file_size)

    if local_file_size != web_file_size:# upgrade available
        upgrade()

### slicetrackgeni wasn't installed, so we download and install it here                
else:
    installation()

if os.path.isdir(Old_slicetrackgeni_directory):
        print('removing slicetrackgeni_exe-master')
        time.sleep(5)
        shutil.rmtree(Old_slicetrackgeni_directory)

print('Launcher up to date')