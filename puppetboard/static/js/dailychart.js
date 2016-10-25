jQuery(function ($) {
  function generateChart(el) {
    var url = "/daily_reports_chart.json";
    var certname = $(el).attr('data-certname');
    if (typeof certname !== typeof undefined && certname !== false) {
        url = url + "?certname=" + certname;
    }
    console.log($(el).attr('id'));
    console.log(certname);
    console.log(url);
  }
  generateChart($("#dailyReportsChart"));
});
