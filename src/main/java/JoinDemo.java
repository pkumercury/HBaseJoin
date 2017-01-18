import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by wen on 17-1-18.
 */
public class JoinDemo {

    public static void generateData() throws IOException{
        // 创建表
        ArrayList<String> tables = new ArrayList<String>();
        tables.add("Person");
        tables.add("Gender");
        tables.add("Hobby");

        ArrayList<ArrayList<String>> families = new ArrayList<ArrayList<String>>();
        ArrayList<String> familyPerson = new ArrayList<String>();
        familyPerson.add("info");
        families.add(familyPerson);

        ArrayList<String> familyGender = new ArrayList<String>();
        familyGender.add("info");
        familyGender.add("gender");
        families.add(familyGender);

        ArrayList<String> familyHobby = new ArrayList<String>();
        familyHobby.add("info");
        familyHobby.add("hobby");
        families.add(familyHobby);

        for (int i = 0; i < tables.size(); i++) {
            HBaseUtil.createTable(tables.get(i), families.get(i));
        }

        Random rd = new Random();
        String[] names = {"aji", "axx", "ldd", "yyf", "zhou", "zei9", "zsmj", "kaka", "bbk", "xxs", "faith",
                "uuu9", "sccc", "lby", "pig", "fy", "super", "chuan", "ch", "hao"};
        String[] genders = {"male", "female"};
        String[] hobbies = {"tennis", "computer", "TV", "basketball", "swim",
                "chess", "sleep", "book", "phone", "eat"};

        // 产生数据
        for (int i = 0; i < 100; i++) {
            int id = rd.nextInt(100);
            int age =  rd.nextInt(100);
            int namePos = id % 20;
            HBaseUtil.addRecord("Person", String.valueOf(i), "info:id", String.valueOf(id));
            HBaseUtil.addRecord("Person", String.valueOf(i), "info:age", String.valueOf(age));
            HBaseUtil.addRecord("Person", String.valueOf(i), "info:name", names[namePos]);
        }

        for (int i = 0; i < 100; i++) {
            int id = rd.nextInt(100);
            String gender = genders[id % 2];
            HBaseUtil.addRecord("Gender", String.valueOf(i), "info:id", String.valueOf(id));
            HBaseUtil.addRecord("Gender", String.valueOf(i), "gender:gender", gender);
        }

        for (int i = 0; i < 100; i++) {
            int namePos = rd.nextInt(20);
            int hobbyPos = rd.nextInt(10);
            HBaseUtil.addRecord("Hobby", String.valueOf(i), "info:name", names[namePos]);
            HBaseUtil.addRecord("Hobby", String.valueOf(i), "hobby:hobby", hobbies[hobbyPos]);
        }
    }

    public static void main(String[] args) {
        // 产生数据
        try {
            generateData();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 待连接的表
        ArrayList<String> tableList = new ArrayList<String>();
        tableList.add("Person");
        tableList.add("Gender");
        tableList.add("Hobby");
        // 连接条件
        ArrayList<String> conditionList = new ArrayList<String>();
        conditionList.add("Person.info:id=Gender.info:id");
        conditionList.add("Person.info:name=Hobby.info:name");

        StringBuilder finalTable = new StringBuilder(tableList.get(0));
        for (int i = 1; i < tableList.size(); i++) {
            finalTable.append("_" + tableList.get(i));
        }
        try {
            HBaseJoin.run(tableList, conditionList);

            HBaseUtil.getAllRecord(finalTable.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
