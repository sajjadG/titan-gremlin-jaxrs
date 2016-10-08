package io.github.sajjadg.java.jaxrs.model;

/**
 * Created by ironman on 10/5/16.
 */
public class Student {

    private double avg;
    private String name;
    private String id;
    private String lname;
    private int age;
    private String bornCity;
    private String bornProvince;
    private String livingCity;
    private String livingProvince;
    private String studyCity;
    private String studyProvince;

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getBornCity() {
        return bornCity;
    }

    public void setBornCity(String bornCity) {
        this.bornCity = bornCity;
    }

    public String getBornProvince() {
        return bornProvince;
    }

    public void setBornProvince(String bornProvince) {
        this.bornProvince = bornProvince;
    }

    public String getLivingCity() {
        return livingCity;
    }

    public void setLivingCity(String livingCity) {
        this.livingCity = livingCity;
    }

    public String getLivingProvince() {
        return livingProvince;
    }

    public void setLivingProvince(String livingProvince) {
        this.livingProvince = livingProvince;
    }

    public String getStudyCity() {
        return studyCity;
    }

    public void setStudyCity(String studyCity) {
        this.studyCity = studyCity;
    }

    public String getStudyProvince() {
        return studyProvince;
    }

    public void setStudyProvince(String studyProvince) {
        this.studyProvince = studyProvince;
    }

}
