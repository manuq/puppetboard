jQuery(function ($) {
  function generateChart(el) {
    console.log(el);
    var certname = $(el).attr('data-certname');
    console.log(certname);
  }
  generateChart($("#dailyReportsChart"));
});
