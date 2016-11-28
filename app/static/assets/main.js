/*********************************
 * HELPER FUNCTIONS              *
 * Animations not related to     *
 * page transitions              *
 *                               *
 *********************************/

function initSearch() {
  $('.search-panel .dropdown-menu').find('a').click(function(e) {
      e.preventDefault();
      var param = $(this).attr("href").replace("#","");
      var category = $(this).text();
      $('.search-panel span#category').text(category);
      $('.input-group #queryFilter').val(param);
  });
  $('form#searchBar .input-group-btn button.submit').click(function() {
      $('form#searchBar').submit()
  });
}

/*********************************
 * PAGE TRANSITIONS              *
 * Powered by smoothstate.js     *
 *                               *
 *********************************/

$(function () {
    'use strict';
    var $page = $('#main'),
        options = {
            debug: true,
            prefetch: false,
            onStart: {
                duration: 330, // Duration of our animation
                render: function ($container) {
                    // Add CSS animation reversing class
                    $container.addClass('is-exiting');
                    // Restart animation
                    smoothState.restartCSSAnimations();
                }
            },
            onReady: {
                duration: 0,
                render: function ($container, $newContent) {
                    // Remove your CSS animation reversing class
                    $container.removeClass('is-exiting');
                    window.scrollTo(0, 0);
                    // Inject the new content
                    $container.html($newContent);
                }
            },
            onAfter: function ($container, $newContent) {
                initSearch();
            }
        },
        smoothState = $page.smoothState(options).data('smoothState');
});

/*********************************
 * SEARCH BAR JS                 *
 * Styled by boostrap.min.css    *
 *                               *
 *********************************/

$(document).ready(function(e){
    initSearch();
});
