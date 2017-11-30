import subprocess
import concurrent.futures
import re
from pathlib import PurePath


def logAndRun(layer):
    rl = r'''"C:\Program Files\QGIS 2.18\bin\ogr2ogr.exe" -f "PostgreSQL" PG:" dbname='uganda_gis' host=localhost port=5432 user='postgres' password='mnzryv' active_schema=import_data" -nln import_data.poles "G:\uganda_data\Uganda.gdb_Oct_25_2017\Uganda.gdb" "%s" -append --config PG_USE_COPY YES -unsetFid  -skipfailures''' % (
        layer)

    print("Import layer {} started".format(layer))
    try:
        p = subprocess.run(rl, shell=True, stderr=subprocess.PIPE, check=True)

        if p.stderr != b'':
            print("Import layer {} had a problem: {}".format(layer, p.stderr))
        else:
            print("Import layer {} completed".format(layer))
    except subprocess.CalledProcessError as e:
        print("layer {} had a problem".format(layer))
        print(e)
        print(p.stderr)


if __name__ == '__main__':
    try:
        proc_layers = subprocess.run(["C:\\Program Files\\QGIS 2.18\\bin\\ogrinfo.exe", "-q",
                                      'G:\\uganda_data\\Uganda.gdb_Oct_25_2017\\Uganda.gdb'], shell=True,
                                     stdout=subprocess.PIPE, check=True)
        out = proc_layers.stdout.decode('ascii')
        layers = re.findall(r'([a-zA-Z]{3}_[a-zA-Z]{3}_Pole) ', out, flags=re.M)
    except subprocess.CalledProcessError as e:
        print(e)
        exit(1)
    print(layers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(logAndRun, layers)


