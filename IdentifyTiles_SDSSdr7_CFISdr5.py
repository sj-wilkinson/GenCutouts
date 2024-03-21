import numpy as np
import multiprocessing as mp
import pymysql, time

def get_tile_from_coords_worker(args):
    
    ra, dec = args
    
    # guess the centre of the corresponding tile from tile naming convention (https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/en/community/unions/MegaPipe_CFIS_DR3.html)
    guess_xxx = ra*2*np.cos(dec*np.pi/180)
    guess_yyy = (dec+90)*2
    
    # round to nearest integer
    guess_xxx = int(np.round(guess_xxx,0))
    guess_yyy = int(np.round(guess_yyy,0))
    
    # all possible guesses (+/- 3 tile)
    guesses_xxx = []
    guesses_yyy = []
    for i in np.arange(-3,4,1):
        for j in np.arange(-3,4,1):
            guesses_xxx.append(guess_xxx+i)
            guesses_yyy.append(guess_yyy+j)
    guesses_xxx = np.array(guesses_xxx)
    guesses_yyy = np.array(guesses_yyy)
    
    # convert guessed tile to ra / dec coords using tile naming convention
    guesses_dec = guesses_yyy/2-90
    guesses_ra = guesses_xxx/2/np.cos(guesses_dec*np.pi/180)
    
    # best tile is the one with the lowest distance from centre of the tile
    #dists = np.sqrt((guesses_dec-dec)**2 + (guesses_ra-ra)**2)
    dists = np.sqrt((guesses_dec-dec)**2 + ((guesses_ra-ra)*np.cos(dec*np.pi/180))**2) # consider trying this afterwards to see if you get a different result
    args = np.argsort(dists)
        
    # report the four closest tiles
    closest_four = np.array([f'CFIS.{guesses_xxx[idx]}.{guesses_yyy[idx]}.r.fits' for idx in args[0:4]])
    
    # load in tile list for referencing
    tiles = np.loadtxt('tile_list_dr5.txt', unpack = True, usecols = [0], dtype = str)
    
    # check if estimated tiles are in the CFIS DR5 tile list
    matched = np.intersect1d(closest_four, tiles)
    
    #if any of the four tiles is in the DR5 tile list, consider it covered
    if len(matched) > 0:
        
        # report the four closest tiles
        out = open('IdentifyTiles_SDSSdr7_CFISdr5_2.out', 'a')
        out.write(f'{objID[0]} {closest_four[0]} {closest_four[1]} {closest_four[2]} {closest_four[3]}\n')
        out.close()


if __name__ == '__main__':
    
    start = time.time()
    
    update_tile_list = False
    if update_tile_list:
        
        # ls all files from CANFAR vault storage
        os.system('vls vos:cfis/tiles_DR5/ > /arc10/swilkinson/CFIS_DR5/tiles_tmp.txt')
        
        # read in listed files
        tile_list = np.loadtxt('/arc10/swilkinson/CFIS_DR5/tiles_tmp.txt', dtype = str)
        
        # if formatted as a tile, add it to the list of tiles
        out = open('/arc10/swilkinson/CFIS_DR5/tile_list_dr5.txt', 'w')
        for tile in tile_list:
            if tile[-6:]=='r.fits':
                out.write(tile+'\n')
        out.close()
    
    # initialize/wipe output file
    out = open('IdentifyTiles_SDSSdr7_CFISdr5_2.out', 'w')
    out.close()
    
    #Query MySQL for CFIS tiles +
    db = pymysql.connect(host = 'lauca.phys.uvic.ca', db = 'sdss', user = 'swilkinson', passwd = '123Sdss!@#')
    x = 'select u.objID, u.ra, u.decl, u.z_spec from dr7_uberuber u WHERE z_spec>0'
    c = db.cursor()
    c.execute(x)
    db_data = c.fetchall()
    c.close()
    db.close()

    db_data = np.array(db_data, dtype = str).T

    objID  = db_data[0]
    ra     = np.array(db_data[1], dtype = float)
    dec    = np.array(db_data[2], dtype = float)
    z_spec = np.array(db_data[3], dtype = float)
    
    inputs = np.array([ra,dec]).T
    
    pool = mp.Pool(12)
    pool.map(get_tile_from_coords_worker, inputs)

    end = time.time()

    print(f'This took {end-start:.1f}s to run.')
    objID_covered = np.loadtxt('IdentifyTiles_SDSSdr7_CFISdr5_2.out', usecols = [0], unpack = True)
    print(f'There is possible CFIS DR5 coverage for {len(objID_covered)} SDSS DR7 targets.')
