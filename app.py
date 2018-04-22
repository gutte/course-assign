#!/usr/bin/python2

#~ 
#~ Script for scheduling courses and assigning students to courses based on indicated preferences
#~ The assignment is done in three steps as follows.
#~ 
#~ STEP 1: Create a shortlist of the most popular courses. The most popular courses may run more than once.
#~ STEP 2: Distribute courses from the shortlist into blocks (groups of courses which run simultaneously and are therefore mutually exclusive)
#~ STEP 3: Assign students to courses/blocks
#~ 

import sys
import os
import random
import agate
import extension 


# DEFAULT PARAMETERS
n_courses = 3                          # number of courses to shortlist
n_blocks = 2                            # number of blocks to use
n_courses_ps = 2                        # number of courses each student should be assigned to



# handle command line arguments

if len(sys.argv) >= 2:
    inputdir = 'input/'+sys.argv[1]+'/'
    outputdir = 'output/'+sys.argv[1]+'/'
    if len(sys.argv) >= 3:
        n_courses = int(sys.argv[2])
        if len(sys.argv) >= 4:
            n_blocks = int(sys.argv[3])
            n_courses_ps = int(sys.argv[3])
else:
    print('Usage: app.py [input dir] [n_of_courses] [n_of_blocks]')
    exit()

    
    
file_preferences = inputdir + 'preferences.csv'
file_courses = inputdir + 'courses.csv'


def output_csv(self,name):
    self.to_csv(outputdir+name+'.csv')
    
agate.Table.output_csv = output_csv




def popularity_function(row):
    return (sum(row[str(i)] for i in range(1, n_blocks + 1)))      # calculate course popularity as the sum courses with highest preferences (first 'n_blocks')


# Read in the preferences table
prefs = agate.Table.from_csv(file_preferences)
# Redo the reading this time forcing number types for all except first column
column_n=len(prefs.column_names)
number_type = agate.Number()
text_type = agate.Text()
column_names = prefs.column_names
column_types = [text_type] + [number_type] * (column_n -1)
prefs = agate.Table.from_csv(file_preferences, column_names, column_types)
#exctract course names into array
course_names = prefs.exclude([prefs.column_names[0],prefs.column_names[1]]).column_names
prefs_n = prefs.normalize('student',course_names) 
prefs_n = prefs_n.rename(column_names = {'property': 'course', 'value': 'preference'})
prefs_n.to_csv('temp.csv')
prefs_n = agate.Table.from_csv('temp.csv')   # workaround to fix .normalize() indexing issue
os.remove('temp.csv')
prefs_n = prefs_n.where(lambda row: row['preference'] != None)


# Read in the courses table

course_params = agate.Table.from_csv(file_courses)


#~ 
#~ STEP 1
#~ 
#~ Create a shortlist of courses by using a popularity ranking based on preferences.
#~ 


# count the preference positions ("preference matrix") for each course
pref_count = prefs_n.pivot('course','preference')
# calculate a course popularity index and rank
pref_count = pref_count \
    .compute([
        ('pop', agate.Formula(agate.Number(), popularity_function))
    ]) \
    .compute([
        ('rank', agate.Rank('pop', reverse=True))
    ]) \
    .order_by('rank')

# reorder the preference columns
pref_count_na=list(pref_count.exclude(['course','pop','rank']).column_names)
pref_count_na.sort()
pref_count_na = ['rank','pop','course'] + pref_count_na
pref_count = pref_count.select(pref_count_na)

#output
pref_count.to_csv(outputdir+'longlist.csv')


# make sure the n_courses matches the length of the course list. For duplicating courses
# add a 'runtimes' column which indicates if columns are to run multiple times

len_courses = len(pref_count.rows)
add_n = n_courses - len_courses

shortlist = pref_count.join(agate.Table([[1]]*len(pref_count.rows), ['repeats'], [agate.Number()])) \
    .select(['rank','course','pop','repeats'])

# functions for adding duplicate instances of courses (repeats)

def can_repeat(self, row_n):
    thiscourse=shortlist.columns['course'][row_n]
    max_repeats=course_params.where(lambda r: r['course']==thiscourse).columns['max_repeats']
    if (len(max_repeats)>0):
        if (max_repeats[0]==None):
            return True
        else:
            if (max_repeats[0] > self.columns['repeats'][row_n]):
                return True
            else:
                return False
    else:
        #shouldnt be here, but just in case
        return True
    

def add_repeat(self, row_n):
    if (self.can_repeat(row_n)):
        self = self.update_where('repeats',self.columns['repeats'][row_n]+1,'course',shortlist.columns['course'][row_n])
        return self
    else:
        return self


agate.Table.add_repeat = add_repeat
agate.Table.can_repeat = can_repeat

# remove courses with max_repeat = 0 (this is a fix for forced removal of courses)
remove_courses = list(course_params.where(lambda r: r['max_repeats']==0).columns['course'])
shortlist = shortlist.order_by(lambda r: r['course'] in remove_courses).limit(len(shortlist)-len(remove_courses))

# sort "must runs" to the top
must_runs = list(course_params.where(lambda r: r['must_run']==1).columns['course'])
shortlist = shortlist.order_by(lambda r: (r['course'] in must_runs,-r['rank']),reverse=True).limit(len(shortlist)-len(remove_courses))

# add a comparison number for the popularity if it is repeated (repeats+1)

# duplicate courses if n_courses is greater than number of courses on list
# or remove courses if n_courses is smaller than number of courses on list

if (add_n > 0):
    row_n = 0
    while (add_n > 0):
        if (shortlist.can_repeat(row_n)):
            shortlist = shortlist.add_repeat(row_n)
            add_n -=1
        row_n += 1
else:
    shortlist = shortlist.limit(n_courses)
    

# duplicate courses if top popularity/2 > lowest ranked course

def comparison_function(row):
    return (row['pop']/(row['repeats']+1))
    
shortlist = shortlist \
    .compute([
        ('comparison_number', agate.Formula(agate.Number(), comparison_function))
    ]) \
    .order_by(lambda r: (r['course'] in must_runs, r['comparison_number']),reverse=True) # order by comparison number but keep "must runs" on top

row_n=0
while (shortlist.columns['comparison_number'][row_n]>shortlist.columns['pop'][len(shortlist.rows)-1]):
    if (shortlist.can_repeat(row_n)):
        # drop last element

        shortlist = shortlist.limit(len(shortlist.rows)-1)
        # repeat shortlist            
        shortlist = shortlist.add_repeat(row_n)
        # recalculate comparison_number
        shortlist = shortlist \
            .update_where('comparison_number',shortlist.columns['pop'][row_n]/(shortlist.columns['repeats'][row_n] + 1),'course',shortlist.columns['course'][row_n]) \
            .order_by('comparison_number',reverse=True)
        # reset row counter
        row_n =0
    else:
        # the course can not be repeated. check next course row
        row_n += 1
        # make sure we dont get stuck
        if (row_n +1 == len(shortlist.rows)):
            break


shortlist = shortlist.select(['rank','course', 'repeats','pop','comparison_number']).order_by('rank')

shortlist.to_csv(outputdir+'shortlist.csv')


#~ TODO STEP 1:
#~ get multiple shortlists
#~ must run courses
#~ course family restrictions


#~ 
#~ STEP 2
#~ 
#~ Group the courses in blocks
#~ (every course within a block will run simultaneously - students can be assigned to one course in each block)
#~ 

# start by ordering.. this is the order in which courses are placed in blocks. Start with non-repeated courses
shortlist = shortlist.order_by(lambda r : (r['repeats'],r['rank']))

courses = shortlist.columns['course'].values()


#create a courslist which has one row for each repeat (instance) of a course
max_repeats = shortlist.order_by('repeats',reverse=True).columns['repeats'][0]
sublists = []
for i in range(1,max_repeats+1):
    sublists.append(shortlist.where(lambda r: r['repeats']>=i).join(agate.Table([[i]]*len(shortlist.rows), ['instance'], [agate.Number()])))
courselist = shortlist.merge(sublists)

# add id column to the courselist
courselist = courselist.join(agate.Table(([[i] for i in range(1,len(courselist.rows)+1)]), ['id'], [agate.Number()]))

# add a blocks column to the courselist
courselist = courselist.join(agate.Table([[None]]*len(courselist.rows), ['block'], [agate.Number()]))

courselist = courselist.select(['id','course','instance','repeats','block']) 

# recalculate student preference to exclude courses excluded
shortprefs = prefs_n.where(lambda row: row['course'] in courses)
shortprefs = shortprefs.group_by('student') \
    .compute([('spreference', agate.Rank('preference'))]) \
    .merge(group_name='extra') \
    .exclude(['extra','preference'])

#join preference table onto itself
joined = shortprefs \
    .join(shortprefs, 'student', 'student', inner=True) \
    .where(lambda row: row['spreference'] != row['spreference2']) \
    .where(lambda row: (row['spreference'] < n_blocks +1 ) and (row['spreference2'] < n_blocks + 1))  #only calculate correlation between 1st and 2nd prefs

#get a table with count of each course combination occuring
pref_corr = joined.pivot(['course','course2']) \
    .rename(column_names={'Count':'corr'}) \
    .order_by('corr', reverse=True)

# pref_corr.print_table()

# first assign block_forced courses                                                      TODO


# generate a table with a column of blocknumbers
blockarray = []
for b in range(1,n_blocks+1):
    blockarray.append([b])
blocksumtable_empty = agate.Table(blockarray,['block'],[agate.Number()])


# put every course in a block
for course in courses:
    #print('course: '+course)
    block_corr_sum = pref_corr.join(courselist,'course2','course') \
        .where(lambda row : row['course'] == course) \
        .pivot('block', aggregation=agate.Sum('corr')) \
        .where(lambda row : row['block'] != None)
    block_corr_sum = blocksumtable_empty.join(block_corr_sum,'block','block') \
        .pivot('block', aggregation=agate.Sum('Sum')) \
        .join(courselist.pivot('block'),'block','block') \
        .order_by(lambda r: (r['Sum'], r['Count']))
    # block_corr_sum.print_table()
    # check the repeats
    bestblocks = block_corr_sum.columns['block']
    repeats = shortlist.where(lambda r: r['course']==course).columns['repeats'][0]
    if repeats>n_blocks:
        repeats=n_blocks
    for instance in range(1, repeats + 1):
        courseid=courselist.where(lambda r: (r['course']==course) and (r['instance'] ==instance)).columns['id'][0]
        courselist = courselist.update_where('block', bestblocks[instance-1],'id',courseid)
        #courselist.print_table()
    
courselist = courselist.select(['id', 'course', 'instance','block']) \
    .order_by('block')
    
courselist.to_csv(outputdir+'/courselist.csv')
  

#~ 
#~ STEP 3
#~ 
#~ Populate the courses with students
#~ 

# we create a selections table with a row for every student and add columns for selected courses (number equal to 'n_courses_ps')
random.seed()

selection_columns = ['selected_'+str(i) for i in range(1,n_courses_ps+1)]

selections = prefs \
    .select(['student','priority']) \
    .join(agate.Table([[None]*n_courses_ps]*len(prefs.rows), selection_columns, [agate.Number()]*n_courses_ps))

# function for selecting a single course (updating the selections table)
def select_course(self,student,selection_n,courseid,priority_change):
    self = self.update_where('selected_'+str(selection_n),courseid,'student',student)
    new_priority = self.where(lambda r: r['student']==student).columns['priority'][0]+priority_change
    self = self.update_where('priority',new_priority,'student',student)
    return self

agate.Table.select_course = select_course

# the main loop for populating courses
for selection_n in range(1,n_courses_ps+1):
    # student order for this round of selection
    selections = selections.order_by(lambda r: (-r['priority'], random.random()))
    # go through all students
    for student_row in range(0, len(selections.rows)):
        student = selections.columns['student'][student_row]
        preferred_courses = list(prefs_n.where(lambda r: r['student']==student).order_by('preference').columns['course'])
        selected_courses = selections \
                .where(lambda r: r['student']==student) \
                .normalize('student', selection_columns) \
                .columns['value']
        selected_courses = [int(i) if i is not None else 0 for i in list(selected_courses)]   #fix to .normalize() problem
        if (selection_n > 1):
            last_selected = selected_courses[selection_n - 2]
            if (last_selected == 0):
                #last round selection failed. this will fail too
                break
            last_selected_pref = preferred_courses.index(courselist.where(lambda r: r['id']==selected_courses[selection_n - 2]).columns['course'][0])
        else:
            last_selected_pref = -1
        # start finding the next course to add
        skipped = last_selected_pref + 1  # n of times we skip to the next preference. Start at 0 for first round of selections
        selected=False # set to true when we have selected the next course
        while (not selected):
            if (skipped >= len(preferred_courses)):
                break
            blocks = list(courselist.where(lambda r: r['course']==preferred_courses[skipped]).columns['block'])
            if (len(blocks)==0):
                #course is not running
                skipped += 1
            else:
                # we check for each block that the preferred course is running in
                block_is_free = [1]*len(blocks)
                blocking_next_pref = [0]*len(blocks)
                students_already_selected = [0]*len(blocks)
                courseid = [None]*len(blocks)
                block_n=0
                for block in blocks:
                    #test if the block is taken
                    courseid[block_n] = courselist.where(lambda r: (r['course']==preferred_courses[skipped]) and (r['block'] == block)).columns['id'][0]                
                    blocks_already_selected = courselist.where(lambda r: r['id'] in selected_courses).columns['block']
                    if (block in list(blocks_already_selected)):
                        block_is_free[block_n]=0
                    else:
                        #the block is free.
                        #is the block full?
                        students_already_selected[block_n]=0
                        for selection_column in selection_columns:
                            students_already_selected[block_n] += len(selections.where(lambda r: r[selection_column]==courseid[block_n]).rows)
                        max_students = course_params.where(lambda r: r['course']==preferred_courses[skipped]).columns['max_students']
                        if (len(max_students)>0 and max_students[0]!=None and students_already_selected[block_n] >= max_students[0]):
                            #FULL
                            block_is_free[block_n]=0
                        else:
                            #not full
                            #check if the block intersects with the block for next preference
                            if (skipped + 1 >= len(preferred_courses)):
                                blocking_next_pref[block_n]=0
                            else:
                                next_pref_blocks = list(courselist.where(lambda r: r['course']==preferred_courses[skipped+1]).columns['block'])
                                if (block in next_pref_blocks):
                                    blocking_next_pref[block_n]=1
                    block_n += 1
                # test if we can select a block
                if (1 in block_is_free):
                    #select the better block
                    free_and_blocking = []
                    for i in range(0,len(blocks)):
                        free_and_blocking.append((i, block_is_free[i], blocking_next_pref[i], students_already_selected[i]))
                    # order of block preference: nonblocking > blocking > block has fewer students for this course > random
                    free_and_blocking = sorted(free_and_blocking, key=lambda s: (-s[1], s[2], s[3], random.random()))
                    which_block=free_and_blocking[0][0]
                    selections = selections.select_course(student,selection_n,courseid[which_block],skipped)
                    selected=True
                else:
                    skipped +=1


#format selections table for output
selected_courses = selections \
    .normalize('student', selection_columns)
selected_courses = selected_courses.rename(column_names = {'property': 'selection', 'value': 'courseid'})

courselist = courselist.join(selected_courses.pivot('courseid'),'id','courseid')

courselist.output_csv('courselist')

selected_courses.to_csv('temp.csv')
selected_courses = agate.Table.from_csv('temp.csv')   # workaround to fix .normalize() indexing issue
os.remove('temp.csv')


selected_courses = selected_courses.join(courselist,'courseid','id') \
    .join(prefs_n,['student','course'],['student','course']) \
    .select(['student','courseid','course','block','preference'])

selected_courses = selected_courses.order_by(lambda r : (r['student'],r['block']))

selected_courses.output_csv('selections_by_student')

selected_courses.select(['course','block','courseid','student']).order_by(lambda r: (r['course'],r['block'])).output_csv('selections_by_course')


# finally output something on the console

    


selected_courses.pivot('block').print_table()


print('Highest preference used:')
selected_courses.pivot('student',aggregation=agate.Max('preference')) \
    .pivot('Max') \
    .order_by('Max') \
    .print_table()
