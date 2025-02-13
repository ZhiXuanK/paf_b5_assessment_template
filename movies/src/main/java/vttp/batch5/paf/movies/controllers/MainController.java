package vttp.batch5.paf.movies.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.json.JsonArray;
import jakarta.servlet.http.HttpSession;
import vttp.batch5.paf.movies.services.MovieService;

@RestController
@RequestMapping("/api")
public class MainController {

  @Autowired
  private MovieService movieSvc;

  // TODO: Task 3
  @GetMapping(path = { "/summary" }, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getProlificDirectors(
      @RequestParam String count,
      HttpSession sess
  ) {

    JsonArray response = movieSvc.getProlificDirectors(Integer.parseInt(count));

    sess.setAttribute("director_table", response);

    return ResponseEntity.ok().body(response.toString());

  }
/*
  // TODO: Task 4
  @GetMapping(path={"/summary/pdf"}, produces=MediaType.APPLICATION_PDF_VALUE)
  public ResponseEntity<String> generatePDF(
    HttpSession sess
  ) throws JRException, FileNotFoundException {
    JsonArray director = (JsonArray) sess.getAttribute("director_table");

    JasperPrint print = movieSvc.generatePDFReport(director);



    return Response.ok(bytes).type("application/pdf").header("Content-Disposition", "filename=report.pdf").build();
  }*/

}
