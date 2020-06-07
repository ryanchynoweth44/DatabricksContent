# CDC Operations within a Delta Lake

Often we develop batch processes the move our data from zone to zone within our delta lake. These batch processes run either on a schedule or a trigger. In order to efficiently process data we are often required to have the ability to only process the changes since we last processed our dataset. 

Delta lake makes it easy to track your processing checkpoints and get the changes that need to be processed. 