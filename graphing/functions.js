function init_graph()
{
  var svg = d3.select("svg");
  var margin = {"top": 30, "bottom": 40, "left": 30, "right": 30};
  var width = +svg.attr("width") - margin.left - margin.right;
  var height = +svg.attr("height") - margin.top - margin.bottom;

  var y = d3.scaleLinear().range([height,0]),
      x = d3.scaleLinear().range([0,width]),
      diam = d3.scalePow().range([2, 75]).exponent(1);

  var container = svg.append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

// For drawing a line, not needed
  var theline = d3.line()
      .x(function(d) { return x(d.sim); } )
      .y(function(d) { return y(d.num_1); } )


  d3.select("#cat_1")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".weight_label").remove()
          graph("1", y, x, diam, container, width, height, theline)
          console.log("button1")
      })

  d3.select("#cat_2")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".y_axis").remove()
          console.log("button2")
          graph("2", y, x, diam, container, width, height, theline)
      })
}

var thedata
function graph(filter, y, x, diam, container, width, height, theline) {

var chart = d3.csv("sample_data.csv", function(data) {

    data = data.filter(function(d) { return d.category == filter; });


    data.forEach(function(data) {
        data.num_1 = +data.num_1;
        data.num_2 = +data.num_2;
        data.category = +data.category;
        data.weight = +data.weight;
        data.sim = +data.sim;
      });


    var x_extent = d3.extent(data, function(data) {return data.sim});


    var y_extent = d3.extent(data, function(data) {return data.num_1});
    var diam_extent = d3.extent(data, function(data) {return data.weight});

    x.domain([x_extent[0] - .1, x_extent[1] * 1.1]);
    y.domain([y_extent[0] - .1 * (y_extent[1] - y_extent[0]) , y_extent[1] * 1.1]);
    diam.domain(diam_extent);
    thedata = data

/*
    container.append("path")
      .data([data])
      .attr("class", "line")
      .attr("d", theline);
*/

    var nodes = container.selectAll(".dot").data(data)
      .enter()
      .filter(function(d) {return d.category == filter});

    // Circles
    nodes.append("circle")
        .attr("class", "dot")
        .attr("cy", function(data) {return y(data.num_1)})
        .attr("cx", function(data) {return x(data.sim)})
        .transition().duration(800)
        .attr("r", function(data) {return diam(data.weight) })
        .attr("fill", "none")
        .attr("stroke", "black")

    // Node Weight Labels
      nodes.append("text")
          .attr("class", "weight_label")
          .attr("x", function(data) { return x(data.sim) - diam(data.weight) - 3; })
          //.transition().duration(1000)
          .attr("y", function(data) { return y(data.num_1); })
          .attr("text-anchor", "end")
          .attr("dy", ".3em")
          .attr("font-size", "12px")
          .text(function(data) {return data.weight});

    // X axis
    container.append("g")
        .attr("class", "x_axis")
        .attr("transform", "translate(" + "0" + "," + height + ")" )
        .call(d3.axisBottom(x))
      .append("text")
        .attr("transform", "translate(" + (width/2) + "," + 34 + ")")
        .attr("text-anchor", "middle")
        .attr("fill", "#000")
        .style("font-size", "15px")
        .text("Cosine Similarity");

    // Y axis
    container.append("g")
        .attr("class", "y_axis")
        .call(d3.axisLeft(y))
      .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("x", -40)
        .attr("dy", "0.71em")
        .attr("text-anchor", "middle")
        .attr("fill", "#000")
        .style("font-size", "15px")
        .text("Other Variable");


    });
}
