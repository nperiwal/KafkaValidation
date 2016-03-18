
import java.io.*;
import java.net.URI;
import java.text.*;
import java.util.*;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.CDL;
import org.json.JSONArray;

import java.nio.charset.StandardCharsets;

/**
 * Created by narayan.periwal on 3/14/16.
 */
public class KafkaScribeValidation {

    public static void main (String args[]) throws Exception {

        String topic = args[0];
        String date = args[1];
        String colo = args[2];
        String offset = args[3];

        System.out.println("Arguments are: " + topic + ", " + date + ", " + colo + ", " + offset);
        String result = validate(topic, date, colo, offset);
        System.out.println("/***********Result************/");
        System.out.println(result);
    }

    public static String validate(String topic, String date, String colo, String offset) throws Exception {
        String hdfsUri;
        if (colo.equals("krypton")) {
            hdfsUri = "webhdfs://krypton-webhdfs.grid.uh1.inmobi.com:14000";
        } else if (colo.equals("topaz")) {
            hdfsUri = "webhdfs://topaz-webhdfs.grid.uj1.inmobi.com:14000";
        } else if (colo.equals("emerald")) {
            hdfsUri = "emerald-webhdfs.grid.lhr1.inmobi.com";
        } else if (colo.equals("opal")) {
            hdfsUri = "opal-webhdfs.grid.hkg1.inmobi.com";
        } else {
            return "Accepted values for colo = opal,krypton,topaz,emerald";
        }


        Date startDate = getStartDate(date);
        long offsetHours  = Long.parseLong(offset);
        Date endDate = getEndDate(date, offsetHours);

        String FS_DEFAULT_NAME = "fs.defaultFS";
        FileSystem fs;
        String basePath = "/kafka/data/validation/" + topic;

        Configuration configuration = new Configuration();
        configuration.setBoolean("fs.automatic.close", false);
        configuration.set(FS_DEFAULT_NAME, hdfsUri);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());

        fs = FileSystem.get(URI.create(hdfsUri), configuration);


        HashMap<String, Measures> data = getAggregatedValues(fs, startDate, endDate, basePath);
        Double measure1Diff, measure2Diff, measure3Diff;
        Double diff1, diff2, diff3;
        Double kafkaMeasure1 = 0.0, kafkaMeasure2 = 0.0, kafkaMeasure3 = 0.0;
        Double scribeMeasure1 = 0.0, scribeMeasure2 = 0.0, scribeMeasure3 = 0.0;
        List values = new ArrayList<>();

        JSONArray docs = new JSONArray();
        for (Map.Entry<String, Measures> entry : data.entrySet()) {
            if (entry.getKey().split("/")[0].equals(date.split("-")[2])) {

                Map<String, String> output = new LinkedHashMap<>();

                kafkaMeasure1 +=  entry.getValue().getKafkaM1();
                kafkaMeasure2 +=  entry.getValue().getKafkaM2();
                kafkaMeasure3 +=  entry.getValue().getKafkaM3();

                scribeMeasure1 +=  entry.getValue().getScribeM1();
                scribeMeasure2 +=  entry.getValue().getScribeM2();
                scribeMeasure3 +=  entry.getValue().getScribeM3();

                measure1Diff = entry.getValue().getScribeM1() - entry.getValue().getKafkaM1();
                measure2Diff = entry.getValue().getScribeM2() - entry.getValue().getKafkaM2();
                measure3Diff = entry.getValue().getScribeM3() - entry.getValue().getKafkaM3();

                output.put("date", entry.getKey());
                output.put("scribeMeasure3", String.valueOf(entry.getValue().getScribeM1()));
                output.put("kafkaMeasure3", String.valueOf(entry.getValue().getKafkaM1()));

                docs.put(output);

                values.add(new Result(entry.getKey(),
                        new ThreeMeasures(entry.getValue().getKafkaM1(), entry.getValue().getKafkaM2(), entry.getValue().getKafkaM3()),
                        new ThreeMeasures(entry.getValue().getScribeM1(), entry.getValue().getScribeM2(), entry.getValue().getScribeM3()),
                        new ThreeMeasures(measure1Diff, measure2Diff, measure3Diff)));
            }
        }

        diff1 = scribeMeasure1 - kafkaMeasure1;
        diff2 = scribeMeasure2 - kafkaMeasure2;
        diff3 = scribeMeasure3 - kafkaMeasure3;
        values.sort(new MySalaryComp());
        values.add(new Result(date, new ThreeMeasures(kafkaMeasure1, kafkaMeasure2, kafkaMeasure3),
                new ThreeMeasures(scribeMeasure1, scribeMeasure2, scribeMeasure3),
                new ThreeMeasures(diff1, diff2, diff3)));

        File file=new File("/home/narayan.periwal/test1.csv");
        String csv = CDL.toString(docs);
        FileUtils.writeStringToFile(file, csv);


        Gson GSON = new GsonBuilder().setPrettyPrinting().create();
        return GSON.toJson(values);
    }

    static class MySalaryComp implements Comparator<Result> {
        @Override
        public int compare(Result r1, Result r2) {
            return r1.date.compareTo(r2.date);
        }
    }

    private static Date getStartDate(String date) throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormatter.parse(date);
    }

    private static Date getEndDate(String date, long offsetHours) throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        return new Date(dateFormatter.parse(date).getTime() + (long)(offsetHours * 60 * 60 * 1000));
    }

    static class ThreeMeasures {
        double m1, m2, m3;

        public ThreeMeasures(double m1, double m2, double m3) {
            this.m1 = m1;
            this.m2 = m2;
            this.m3 = m3;
        }
    }

    static class Result {
        String date;
        ThreeMeasures kafkaMeasure;
        ThreeMeasures scribeMeasure;
        ThreeMeasures diff;

        public Result(String date, ThreeMeasures kafkaMeasure, ThreeMeasures scribeMeasure, ThreeMeasures diff) {
            this.date = date;
            this.kafkaMeasure = kafkaMeasure;
            this.scribeMeasure = scribeMeasure;
            this.diff = diff;
        }
    }

    static class Measures {
        double scribeM1;
        double scribeM2;
        double scribeM3;
        double kafkaM1;
        double kafkaM2;
        double kafkaM3;

        public double getKafkaM1() {
            return kafkaM1;
        }

        public double getKafkaM2() {
            return kafkaM2;
        }

        public double getKafkaM3() {
            return kafkaM3;
        }

        public double getScribeM1() {
            return scribeM1;
        }

        public double getScribeM2() {
            return scribeM2;
        }

        public double getScribeM3() {
            return scribeM3;
        }

        Measures() {
            scribeM1 = 0.0;
            scribeM2 = 0.0;
            scribeM3 = 0.0;
            kafkaM1 = 0.0;
            kafkaM2 = 0.0;
            kafkaM3 = 0.0;
        }

        void scribeAdd(double a, double b, double c) {
            this.scribeM1 += a;
            this.scribeM2 += b;
            this.scribeM3 += c;
        }

        void kafkaAdd(double a, double b, double c) {
            this.kafkaM1 += a;
            this.kafkaM2 += b;
            this.kafkaM3 += c;
        }
    }

    private static HashMap<String, Measures> getAggregatedValues(FileSystem fs, Date startDate, Date endDate,
                                                                 String basePath) throws IOException {

        long ONE_MIN = 60000;

        HashMap<String, Measures> data = new HashMap<>();

        while (startDate.before(endDate)) {
            Format formatter = new SimpleDateFormat("/yyyy/MM/dd/HH");
            Path scribePath = new Path(basePath + formatter.format(startDate) + "/scribe/part-r-00000");
            System.out.println("Reading from " + scribePath);
            BufferedReader br = null;
            try {
                br = new BufferedReader(new InputStreamReader(fs.open(scribePath), StandardCharsets.UTF_8));
            } catch (IOException e) {
                startDate.setTime(startDate.getTime() + (long) (ONE_MIN * 60l));
                System.out.println("IOException encountered while reading from " + scribePath);
                continue;
            }
            String line;
            line = br.readLine();
            while (line != null) {
                String[] splitIt = line.split(",");
                try {
                    if (data.containsKey(splitIt[0])) {
                        data.get(splitIt[0]).scribeAdd(Double.parseDouble(splitIt[1]), Double.parseDouble(splitIt[2]), Double.parseDouble(splitIt[3]));
                    } else {
                        data.put(splitIt[0], new Measures());

                        data.get(splitIt[0]).scribeAdd(Double.parseDouble(splitIt[1]), Double.parseDouble(splitIt[2]), Double.parseDouble(splitIt[3]));

                    }
                } catch (NumberFormatException e) {
                    //e.printStackTrace();
                    System.out.print("#,");
                }
                line = br.readLine();
            }


            Path kafkaPath = new Path(basePath + formatter.format(startDate) + "/kafka/part-r-00000");
            System.out.println("Reading from " + kafkaPath);

            try {
                br.close();
                br = new BufferedReader(new InputStreamReader(fs.open(kafkaPath),StandardCharsets.UTF_8));
            } catch (IOException e) {
                startDate.setTime(startDate.getTime() +(long)( ONE_MIN * 60l));
                System.out.println("IOException encountered while reading from " + kafkaPath);
                continue;
            }
            line = br.readLine();
            while (line != null) {
                String[] splitIt = line.split(",");
                try {
                    if (data.containsKey(splitIt[0])) {
                        data.get(splitIt[0]).kafkaAdd(Double.parseDouble(splitIt[1]), Double.parseDouble(splitIt[2]), Double.parseDouble(splitIt[3]));
                    } else {
                        data.put(splitIt[0], new Measures());
                        data.get(splitIt[0]).kafkaAdd(Double.parseDouble(splitIt[1]), Double.parseDouble(splitIt[2]), Double.parseDouble(splitIt[3]));
                    }
                } catch (NumberFormatException e) {
                    //e.printStackTrace();
                    System.out.print("#,");
                }
                line = br.readLine();
            }
            startDate.setTime(startDate.getTime() + (long) (ONE_MIN * 60l));
            br.close();
        }

        return data;
    }

}
