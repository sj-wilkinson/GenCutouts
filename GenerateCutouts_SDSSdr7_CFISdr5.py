import os, sys, pymysql, glob
import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import wcs
from astropy.nddata.utils import Cutout2D
from astropy.coordinates import ICRS 
from astropy.cosmology import Planck18 as cosmo
import astropy.units as u
from multiprocessing import Pool


def GenerateCutout_worker(args):
    
    i,r,d,z,t1,t2,t3,t4 = args
    r = float(r)
    d = float(d)
    z = float(z)
    kpc = 150
    
    # implicit assumption that dr5 tile list is inclusive of lsb list
    t1.replace("CFIS", "CFIS_LSB")
    t2.replace("CFIS", "CFIS_LSB")
    t3.replace("CFIS", "CFIS_LSB")
    t4.replace("CFIS", "CFIS_LSB")

    # set up paths
    tpath = '/arc10/swilkinson/CFIS_DR5/tiles/' # path to tiles 
    ipath = '/arc10/swilkinson/CFIS_DR5/cutouts_lsb/' # path to image cutouts
        
    # cutout size in pixels
    npix = get_npix_from_z(z, size_in_kpc = kpc, pixel_scale = 0.187)

    # check if estimated tiles are in the CFIS DR5 tile list
    closest_four = np.array([t1,t2,t3,t4])
    tiles = np.loadtxt('/arc10/swilkinson/CFIS_DR5/tile_list_dr5_lsb.txt', dtype = str) # implicit assumption that dr5 tile list is inclusive of lsb list
    matched, idx, idx2 = np.intersect1d(closest_four, tiles, return_indices = True)
    
    print(len(matched), tiles[0], closest_four, closest_four[idx])

    tiles_to_check = closest_four[idx]
      
    for i in range(len(tiles_to_check)):
            
        t = tiles_to_check[i]
            
        # generate cutout
        image = cutout_from_tile(t,r,d,npix,tpath)
            
        # generate + update header
        if np.sum(image.flatten()) != 0:

            header = fits.getheader(tpath+t) # (iteratively replace with most recent tile)
            header  = add_to_header(header, kpc, z, i)
            
        else:
            continue

        # check actual on-sky coverage
        edge1_empty = np.sum(image[0,:]) == 0
        edge2_empty = np.sum(image[-1,:]) == 0
        edge3_empty = np.sum(image[:,0]) == 0
        edge4_empty = np.sum(image[:,-1]) == 0
        blank_fraction = np.sum(image.flatten()==0)/len(image.flatten())
        negative_fraction = np.sum(image.flatten()<-15)/len(image.flatten())

        if edge1_empty | edge2_empty | edge3_empty | edge4_empty | (blank_fraction > 0.3) | (negative_fraction > 0.3):
                
            continue
                
        else:
                
            #save as fits
            fname = ipath+i+f'_CFIS_LSB_image_{kpc}kpc.fits'
            fits.writeto(fname,image,header=header,overwrite = True)

            return
    

def get_cutout_from_tile(t, r, d, npix, tpath):

    if not os.path.isfile(tpath+t):
        os.system('vcp vos:cfis/tiles_LSB_DR5/'+t+' '+tpath)

    header = fits.getheader(tpath+t)
    data = np.array(fits.getdata(tpath+t), dtype = 'float')
    w = wcs.WCS(header)
    c = SkyCoord(r,d, frame='icrs',unit='deg')
    dim = (npix,npix)
    
    try:
        image = Cutout2D(data, c, dim, wcs = w, mode = 'partial', fill_value = 0).data # closest tile cutout
        
    except:
        print('Arrays do not overlap. Try next-closest tile.')
        image = np.zeros(dim)
        
    return image


def get_npix_from_z(z, size_in_kpc, pixel_scale):
    
    arcsec_per_kpc = cosmo.arcsec_per_kpc_proper(z).value
    size_in_arcsec = size_in_kpc * arcsec_per_kpc
    size_in_pixels = size_in_arcsec / pixel_scale
    
    return int(np.round(size_in_pixels, 0))


def get_tile_from_coords(ra, dec, tiles):
    
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
    dists = np.sqrt((guesses_dec-dec)**2 + (guesses_ra-ra)**2)
    #dists = np.sqrt((guesses_dec-dec)**2 + ((guesses_ra-ra)*np.cos(dec*np.pi/180))**2) # consider trying this afterwards to see if you get a different result
    args = np.argsort(dists)
        
    # report the four closest tiles
    closest_four = np.array([f'CFIS.{guesses_xxx[idx]}.{guesses_yyy[idx]}.r.fits' for idx in args[0:4]])
    
    # check if estimated tiles are in the CFIS DR5 tile list
    matched = np.intersect1d(closest_four, tiles)
    
    #if any of the four tiles is in the DR5 tile list, consider it covered
    if len(matched) > 0:
        
        # return the four closest tiles
        return closest_four 
    
    else:
        
        return None, None, None, None   
    
    
def add_to_header(header, kpc, i):
    
    header['INFO'] = 'CFIS DR5 LSB cutout generated by S. Wilkinson'
    header['OBJID'] = (i, 'SDSS Object ID')
    header['COSMO']= ('Planck18', 'Cosmology used to calculate cutout size')
    header['SIZE_KPC'] = (kpc, 'Size of cutout side length [kpc]')
    
    return header


if __name__ == '__main__':
    
    
    # load in objID and tiles from IdentifyTiles
    objID_covered, t1, t2, t3, t4 = np.loadtxt('/astro/swilkinson/Desktop/CFIS/GenCutouts/IdentifyTiles_SDSSdr7_CFISdr5_2.out', unpack = True, dtype = str)

    print(f'There is possible CFIS DR5 coverage for {len(objID_covered)} SDSS DR7 targets.')
    
    #Query MySQL for SDSS positions and redshifts
    db = pymysql.connect(host = 'lauca.phys.uvic.ca', db = 'sdss', user = 'swilkinson', passwd = '123Sdss!@#')
    x = 'select u.objID, u.ra, u.decl, u.z_spec from dr7_uberuber u WHERE z_spec>0'
    c = db.cursor()
    c.execute(x)
    db_data = c.fetchall()
    c.close()
    db.close()

    db_data = np.array(db_data, dtype = str).T

    objID = db_data[0]
    ra    = np.array(db_data[1], dtype = float)
    dec   = np.array(db_data[2], dtype = float)
    z     = np.array(db_data[3], dtype = float)

    # match to those with coverage
    matched, idx_table, idx_covered = np.intersect1d(objID, objID_covered, return_indices = True)

    objID = objID[idx_table]
    ra    = ra[idx_table]
    dec   = dec[idx_table]
    z     = z[idx_table]
    
    objID_covered = objID_covered[idx_covered]
    t1 = t1[idx_covered]
    t2 = t2[idx_covered]
    t3 = t3[idx_covered]
    t4 = t4[idx_covered]
    
    print(f'Q: Are my arrays correctly aligned? A: {np.all(objID_covered==objID)}')
    
    '''
    # sort by tiles to reduce downloading... 
    args  = np.argsort(t1)
    objID = objID[args]
    ra    = ra[args]
    dec   = dec[args]
    z     = z[args]
    t4    = t4[args]
    t3    = t3[args]
    t2    = t2[args]
    t1    = t1[args]
    '''
    
    inputs = np.array([objID,ra,dec,z,t1,t2,t3,t4])
    
    print('Start looping through tiles...')
    
    for i, primary_tile in enumerate(np.unique(t1)):

        inputs_on_this_tile = inputs[:,t1==primary_tile].T
        
        pool = Pool(4)
        pool.map(GenerateCutout_worker, inputs_on_this_tile)
        
        if i%20==0:
            # clear downloaded tiles every 20th tile ... 
            os.system('rm -rf /arc10/swilkinson/CFIS_DR5/tiles/')

       
       