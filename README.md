# P1
Developed by Justis Crocker
## Project Prompt
- Problem Scenario 1 
-- What is the total number of consumers for Branch1?
-- What is the number of consumers for the Branch2?
-- Type 1: Creating single physical table with sub queries.
-- Type 2: Creating multiple physical tables
-- "use any one type which you are comfortable"

- Problem Scenario 2 
-- What is the most consumed beverage on Branch1
-- What is the least consumed beverage on Branch2
-- What is the Average consumed beverage of  Branch2

- Problem Scenario 3
-- What are the beverages available on Branch10, Branch8, and Branch1?
what are the comman beverages available in Branch4,Branch7?

- Problem Scenario 4
create a partition,View for the scenario3.

- Problem Scenario 5
Alter the table properties to add "note","comment"
Remove a row from the any Senario.

- Problem Scenario 6
Add future query

- Please add visualization as well.
-- Technology to use:
1. Hive
2. Spark
3. Scala

- All the quaries should be and app  with menu

- Start date :01-02-2022 to 11-02-2022

## Technologies Used
1. Spark 2.3.1
2. SBT 1.3.10

## Features
- Great dynamic menu! Easy to add new menus.
- Verified Input
- Saves future query to HDFS.

## Getting Started
1. Install SBT
   - If using IntelliJ, install Scala language support plugin (just search Scala in the plugin marketplace). Lookup Scala and SBT support for your IDE
   - Otherwise, install SBT and use SBT commands for testing, packaging, compilation and running the project.
2. Get the code
   - Clone the main branch with 'git clone github.com/RevJustis/P1'
3. Open and install dependencies
   - Open the project in your IDE, it may ask you to sync or resync SBT. Do this, as it will install the specified dependencies in the build.sbt file.
   - You can manually resync with this command 'sbt update' Remember to navigate to the project directory!

## Usage
It is a simple program, so just follow the menu after running! Use your IDE run button or this command:
'sbt run'
This command will first compile/recompile, and then execute the program.
