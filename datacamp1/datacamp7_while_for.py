#while loop
# Initialize offset
offset = 8

# Code the while loop
while offset != 0 :
        print("correcting...")
        offset = offset -1
        print(offset)

# Initialize offset
offset = -6

print(" ")
print("--------next code part--------")
print(" ")

# Code the while loop
while offset != 0 :
    print("correcting...")
    if offset > 0 :
      offset = offset - 1
    else :
      offset = offset + 1
    print(offset)

#end while loop

#for loop
fam = [1.73,1.68,1.71,1.89]

for height in fam:
    print(height)

print(" ")
print("--------next code part--------")
print(" ")

for index, height in enumerate(fam):
    print("index "+ str(index) + ": " + str(height))

print(" ")
print("--------next code part--------")
print(" ")

for c in "family":
    print(c.capitalize())


# areas list
areas = [11.25, 18.0, 20.0, 10.75, 9.50]

# Code the for loop
for val in areas:
    print(val)

# areas list
areas = [11.25, 18.0, 20.0, 10.75, 9.50]

# Code the for loop
for index, area in enumerate(areas) :
    print("room " + str(index +1) + ": " + str(area))

# house list of lists
house = [["hallway", 11.25],
         ["kitchen", 18.0],
         ["living room", 20.0],
         ["bedroom", 10.75],
         ["bathroom", 9.50]]

# Build a for loop from scratch
for room in house:
    print("the " + room[0] + " is " + str(room[1]) + " sqm")

#end for loop

#loop datastructures
# Definition of dictionary
europe = {'spain': 'madrid', 'france': 'paris', 'germany': 'berlin',
          'norway': 'oslo', 'italy': 'rome', 'poland': 'warsaw', 'austria': 'vienna'}

# Iterate over europe
for key, value in europe.items():
    print("the capital of " + key + " is " + value)

    #numpy arrays
    # Import numpy as np
    import numpy as np

    print(np_height)
    # For loop over np_height
    for height in np_height:
        print(str(height) + " inches")

    # For loop over np_baseball with nditer()
    print(np_baseball)
    for base in np.nditer(np_baseball):
        print(base)

print(" ")
print("--------next code part--------")
print(" ")

print("iterrows()")
# Import cars data
import pandas as pd
cars = pd.read_csv('cars.csv', index_col = 0)

# Iterate over rows of cars
for lab, row in cars.iterrows():
    print(lab)
    print(row)

# Import cars data
import pandas as pd
cars = pd.read_csv('cars.csv', index_col = 0)

# Adapt for loop
for lab, row in cars.iterrows() :
    print(str(lab)+": " + str(row["cars_per_cap"]))

# Import cars data
import pandas as pd
cars = pd.read_csv('cars.csv', index_col = 0)

# Code for loop that adds COUNTRY column
for lab, row in cars.iterrows():
    cars.loc[lab,"COUNTRY"]= row["country"].upper()

# Import cars data
import pandas as pd
cars = pd.read_csv('cars.csv', index_col = 0)

# Use .apply(str.upper)
cars["COUNTRY"]=cars["country"].apply(str.upper)

print(cars)

# Print cars
print(cars)


#end loop datastructures