#!/bin/bash

monthday=(31 -1 31 30 31 30 31 31 30 31 30 31)
fromdate=""
todate=""
year=0
month=0
day=0

MonthDifference()
{
	increment=0
	fromdate=$1
	todate=$2
	fromdate=`date -d $fromdate +%Y-%m-%d`
	todate=`date -d $todate +%Y-%m-%d`
	echo "After parsing fromdate is: " $fromdate " And todate is: " $todate
	#comparing two dates and assign accordingly	
	if [ `date -d"${fromdate}" +%Y%m%d%H%M%S` -gt `date -d"${todate}" +%Y%m%d%H%M%S` ]; then
	todate=$fromdate
	fromdate=$todate
	fi
	#Getting day of month
	dayOfMonthOfFromDate=`date -d $fromdate +%d`	
	dayOfMonthOfToDate=`date -d $todate +%d`
	yearOfFromDate=`date -d $fromdate +%Y`
	yearOfToDate=`date -d $todate +%Y`
	monthOfYearOfFromDate=`date -d $fromdate +%m`
	monthOfYearOfToDate=`date -d $todate +%m`
	
	#Removing starting 0 from the number
	dayOfMonthOfFromDate=${dayOfMonthOfFromDate#0}
	dayOfMonthOfToDate=${dayOfMonthOfToDate#0}
	yearOfFromDate=${yearOfFromDate#0}
	yearOfToDate=${yearOfToDate#0}
	monthOfYearOfFromDate=${monthOfYearOfFromDate#0}
	monthOfYearOfToDate=${monthOfYearOfToDate#0}
	
	echo "dayOfMonthOfFromDate " $dayOfMonthOfFromDate ", dayOfMonthOfToDate " $dayOfMonthOfToDate ", yearOfFromDate " $yearOfFromDate ", monthOfYearOfFromDate " $monthOfYearOfFromDate ", monthOfYearOfToDate " $monthOfYearOfToDate
	isFromDateLeapyear=$(($(($(($yearOfFromDate & 3)) == 0)) && $(($(($(($yearOfFromDate % 100)) != 0)) || $(($(($yearOfFromDate % 400)) == 0))))))
	echo "Is " $yearOfFromDate " leap year: " $isFromDateLeapyear	
	if [ $dayOfMonthOfFromDate -gt $dayOfMonthOfToDate ] ; then
	index=$(( $monthOfYearOfFromDate - 1 ))
	echo "index is " $index
	increment=${monthday[$index]}
	fi
	if [ $increment -eq "-1" ]; then
		if [ $isFromDateLeapyear ]; then			
			increment=29
		else
			increment=28
		fi
	fi
	echo "Increment is " $increment
	if [ $increment -ne "0" ]; then
		day=$(( ( $dayOfMonthOfToDate + $increment ) - $dayOfMonthOfFromDate ))
		echo "Inside if" $day
		increment=1
	else
		day=$(( $dayOfMonthOfToDate - $dayOfMonthOfFromDate ))
		echo "Inside else " $day
	fi
	if [ $(( $monthOfYearOfFromDate + $increment )) -gt $monthOfYearOfToDate ] ; then
		month=$(( ( $monthOfYearOfToDate + 12) - ( $monthOfYearOfFromDate + $increment ) ))
		increment=1
	else
		month=$(( $monthOfYearOfToDate - ( $monthOfYearOfFromDate + $increment ) ))
		increment=0
	fi
	year=$(( $yearOfToDate - ( $yearOfFromDate + $increment ) ))
	echo "Final Result"
	echo $year " years" $month " months" $day " days"		
}

#calling month difference function
MonthDifference $1 $2
