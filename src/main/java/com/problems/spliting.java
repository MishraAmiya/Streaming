package com.problems;

import org.apache.hadoop.security.SaslOutputStream;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by AMIYA on 9/30/2018.
 */
public class spliting {
    public static void main(String[] args) {
        List<String> finalvalue = partionAfterToWords("aaa,bbb,ccc,ddd,eee,fff,ggg");
        for(String values : finalvalue){
            System.out.println(values);
        }
    }

    private static List<String> partionAfterToWords(String s) {
        List<String> finalValues = new ArrayList<>();
        String[] values = s.split(",");
        int counter=0;
        String value="";
        for(int i =0 ; i < values.length; i++){
             counter++;
             if(value.isEmpty())
                 value =  " \""+  values[i] + "\" ";
             else
                 value = value + ","+" \""+  values[i] + "\" ";
             if(counter == 2 || i == values.length -1){
                 finalValues.add("{ " + value + " }");
                 counter=0;
                 value="";
             }
        }
        return finalValues;
    }
}
