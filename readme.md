Script for scheduling courses and assigning students to courses based on indicated preferences when sceduling restrictions are severe.

Using Python and the agate module.

## Problem description

Students are provided with a list of "elective short courses" and indicate their selection in order of preference (with some reserve choices).
Our job is to optimise the assignment of courses by a) deciding which courses will run and b) assigning students to the courses.

The scheduling restrictions are severe:

- all students must be assigned to a certain number of courses
- all students must be attending a course at the same time, i.e. no free periods (limits degrees of freedom)
- some courses share resources (rooms or teachers) and may therefore not run simultaneously
- there is a maximum number of total courses we may run
- courses may have a maximum number of students

Some degrees of freedom are provided by the fact that courses may run in several instances.

## Running the program

Usage: app.py [input_dir] [n_of_courses] [n_of_blocks]

Additionally the directory **input/[input_dir]**/ must contain data files *preferences.csv* and *courses.csv*, the format of which can be found in  **input/testdata**/.
