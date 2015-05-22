from stats import Stats

def main():
    means = Stats()
    means.getMeans()
    movieMean = means.getMoveMeans()
    print ( "movie mean: " + str(means.moviesMean))
    print ( "music mean: " + str(means.musicMean))
    print ( "games mean: " + str(means.gamesMean))
    print ("Single method for movie means: " + str(movieMean))
    
    
if __name__ == "__main__":
    main()
    
        
        
        
        
        
