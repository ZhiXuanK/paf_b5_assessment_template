package vttp.batch5.paf.movies.repositories;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.stereotype.Repository;

import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.Utils;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private Utils utils;

    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    //
    /*
     * db.imdb.insertMany([
     * {
     * _id: 'tt1259521',
     * title: 'The Cabin in the Woods',
     * directors: 'Drew Goddard',
     * overview: 'A group of teens journey to a remote cabin in the woods where
     * their fate is unknowingly controlled by technicians as part of a worldwide
     * conspiracy where all horror movie clichés are revealed to be part of an
     * elaborate sacrifice ritual.',
     * tagline: 'You think you know the story.',
     * genres: 'Horror, Mystery',
     * imdb_rating: 7,
     * imdb_votes: 465176
     * },
     * {
     * _id: 'tt1259522',
     * title: 'Sample',
     * directors: 'Goddard',
     * overview: 'A group of teens journey to a remote cabin in the woods where
     * their fate is unknowingly controlled by technicians as part of a worldwide
     * conspiracy where all horror movie clichés are revealed to be part of an
     * elaborate sacrifice ritual.',
     * tagline: 'You think you know the story.',
     * genres: 'Horror, Mystery',
     * imdb_rating: 7,
     * imdb_votes: 465176
     * }
     * ]);
     */
    public void batchInsertMovies(List<JsonObject> obj) {

        List<Document> movies = new LinkedList<>();

        for (JsonObject o : obj) {
            System.out.println(o.toString());
            Document movie = new Document()
                    .append("_id", utils.checkString(o, "imdb_id"))
                    .append("title", utils.checkString(o, "title"))
                    .append("directors", utils.checkString(o, "director"))
                    .append("overview", utils.checkString(o, "overview"))
                    .append("tagline", utils.checkString(o, "genres"))
                    .append("imdb_rating", utils.checkFloat(o, "imdb_rating"))
                    .append("imdb_votes", utils.checkInt(o, "imdb_votes"));

            movies.add(movie);
        }

        mongoTemplate.insert(movies, "imdb");

    }

    // TODO: Task 2.4
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    /* 
    db.imdb.insert(
     * {
     * imdb_ids: 'tt1259521',
     * error: 'The Cabin in the Woods',
     * timestamp: 'Drew Goddard'
     * }
     * );
    */
    // native MongoDB query here
    //
    public void logError(List<JsonObject> batch, String error, Date timestamp) {

        int size = batch.size();

        String[] ids = new String[size];

        for (int i=0; i<size; i++){
            ids[i] = batch.get(i).getString("imdb_id");
        }

        Document document = new Document()
            .append("imdb_ids", ids)
            .append("error", error)
            .append("timestamp", timestamp);

        mongoTemplate.insert(document, "errors");

    }

    // TODO: Task 3
    // Write the native Mongo query you implement in the method in the comments
    /*
        db.imdb.aggregate([
        {$project: {
            directors: {
                $split: ["$directors", ","]
            } 
        }},
        {
            $unwind: "$directors"
        },
        {
            $group: {
                _id: "$directors",
                count: {$sum: 1},
                imdb_id: {$push: "$_id"}
            }
        },
        {
            $sort: {
                count: -1
            }
        },
        {
            $limit: 8
        }
        ]);
     */
    // native MongoDB query here
    //
    public List<Document> getTopDirectors(int n){

        
        ProjectionOperation projectOps = Aggregation.project("_id")
            .and(AggregationExpression.from(MongoExpression.create("""
                $split: ["$directors", ","]
                """))).as("directors");
        AggregationOperation unwindOps = Aggregation.unwind("directors");

        GroupOperation groupOps = Aggregation.group("directors")
            .count().as("count")
            .push("_id").as("imdb_id");

        SortOperation sortOps = Aggregation.sort(Sort.by(Direction.DESC, "count"));
        LimitOperation limitOps = Aggregation.limit(n);

        Aggregation pipeline = Aggregation.newAggregation(projectOps, unwindOps, groupOps, sortOps, limitOps);

        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, "imdb", Document.class);

        return results.getMappedResults();

    }

}
