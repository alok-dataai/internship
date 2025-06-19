import sqlite3 as sq
con=sq.connect("mydb.db")
cur=con.cursor()

#Creating table student
#cur.execute("Create table student(studentid varchar(25),studentname varchar(25),studentage numeric(20))" )




#Insert 5 records on user input
#for i in range(5):
##    id=input("Enter the id:")
##    name=input("Enter the name:")
##    age=int(input("Enter the age:"))
##    cur.execute(f"insert into student values('{id}','{name}',{age})")
##    con.commit()
    

# Print all the data from student table
##cur.execute("Select * from student")
##data=cur.fetchall()
##print(data)


# Accept a studentid from user and update studentname and studentage.
##id=input("Enter the id:")
##cur.execute(f"Update student set studentname='Rajeev' and studentage=25 where studentid='{id}' ")
##con.commit()


# Accept a studentid from user and delete what record.
##id=input("Enter the id:")
##cur.execute(f"Delete from student where studentid='{id}' ")
##con.commit()


con.close()






