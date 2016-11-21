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
