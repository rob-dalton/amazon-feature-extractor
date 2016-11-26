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
    $('.search-panel .dropdown-menu').find('a').click(function(e) {
        e.preventDefault();
        var param = $(this).attr("href").replace("#","");
        var concept = $(this).text();
        $('.search-panel span#search_concept').text(concept);
        $('.input-group #search_param').val(param);
    });
});
