function init_graph(csv)
{
  var svg = d3.select("svg");
  var margin = {"top": 30, "bottom": 40, "left": 30, "right": 30};
  var width = +svg.attr("width") - margin.left - margin.right;
  var height = +svg.attr("height") - margin.top - margin.bottom;

  var y = d3.scaleLinear().range([height,0]),
      x = d3.scaleLinear().range([0,width]),
      diam = d3.scalePow().range([0, 14]).exponent(.7);

  var container = svg.append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  d3.select("#cat_0")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".x_axis").remove()
          console.log(1)
          graph("0", y, x, diam, container, width, height, "% Uppercase", "Product Rank", csv)
      })

  d3.select("#cat_1")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".x_axis").remove()
          console.log(1)
          graph("0", y, x, diam, container, width, height, "Title Length", "Product Rank", csv)
      })

  d3.select("#cat_2")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".x_axis").remove()
          console.log(2)
          graph("2", y, x, diam, container, width, height, "% Uppercase", "Overall Rating", csv)
      })

  d3.select("#cat_3")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".x_axis").remove()
          console.log(3)
          graph("3", y, x, diam, container, width, height, "Title Length", "Overall Rating", csv)
      })

  d3.select("#cat_4")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".x_axis").remove()
          console.log(4)
          graph("4", y, x, diam, container, width, height, "Total Votes", "Review Length", csv)
      })

  d3.select("#cat_5")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".x_axis").remove()
          console.log(5)
          graph("5", y, x, diam, container, width, height, "Helpful Votes", "Review Length", csv)
      })

  d3.select("#cat_6")
      .on("click", function(d,i) {
          container.selectAll(".dot").remove()
          container.selectAll(".weight_label").remove()
          container.selectAll(".y_axis").remove()
          container.selectAll(".x_axis").remove()
          console.log(6)
          graph("6", y, x, diam, container, width, height, "Price", "Stars", csv)
      })
}

var thedata
function graph(filter, y, x, diam, container, width, height, x_label, y_label, csv) {

var chart = d3.csv(csv, function(data) {
  console.log(data)

    data = data.filter(function(d) { return d.category == filter; });


    data.forEach(function(data) {
        //console.log(data.x_var)
        //console.log(data.y_var)
        data.x_var = +data.x_var;
        data.y_var = +data.y_var;
        data.category = +data.category;
        data.weight = +data.weight;
      });



    var x_extent = d3.extent(data, function(data) {return data.x_var});
    var y_extent = d3.extent(data, function(data) {return data.y_var});
    var diam_extent = d3.extent(data, function(data) {return data.weight});

    x.domain([x_extent[0] - .1, x_extent[1] * 1.1]);
    y.domain([y_extent[0] - .1 * (y_extent[1] - y_extent[0]) , y_extent[1] * 1.1]);
    diam.domain(diam_extent);
    thedata = data


    var nodes = container.selectAll(".dot").data(data)
      .enter()

    // Circles
    nodes.append("circle")
        .attr("class", "dot")
        .attr("cy", function(data) {return y(data.y_var)})
        .attr("cx", function(data) {return x(data.x_var)})
        .transition().duration(800)
        .attr("r", function(data) {return diam(data.weight) })
        .attr("fill", "none")
        .attr("stroke", "black")

    // Node Weight Labels
    /*
      nodes.append("text")
          .attr("class", "weight_label")
          .attr("x", function(data) { return x(data.x_var) - diam(data.weight) - 3; })
          //.transition().duration(1000)
          .attr("y", function(data) { return y(data.y_var); })
          .attr("text-anchor", "end")
          .attr("dy", ".3em")
          .attr("font-size", "12px")
          .text(function(data) {return data.weight});
      */
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
        .text(x_label);

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
        .text(y_label);


    });
}
