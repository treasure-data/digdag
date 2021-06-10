package io.digdag.guice.rs.server;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.digdag.commons.guava.ThrowablesUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class ServerLifeCycleManager
{
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private static class ManagedInstance
    {
        final Object object;
        List<Method> postStartMethods;
        final List<Method> preStopMethods;

        ManagedInstance(Object object, List<Method> postStartMethods, List<Method> preStopMethods)
        {
            this.object = object;
            this.postStartMethods = postStartMethods;
            this.preStopMethods = preStopMethods;
        }

        boolean postStartCalled()
        {
            postStartMethods = ImmutableList.of();
            return preStopMethods.isEmpty();
        }
    }

    private final Queue<ManagedInstance> managedInstances = new ConcurrentLinkedQueue<ManagedInstance>();

    private enum State
    {
        LATENT(0),
        STARTING(1),
        STARTED(2),
        STOPPING(3),
        STOPPED(4);

        private final int index;

        private State(int index)
        {
            this.index = index;
        }

        public boolean isSameOrAfter(State another)
        {
            return another.index <= index;
        }
    }

    public void manageInstance(Object obj)
            throws Exception
    {
        State currentState = state.get();
        if (currentState.isSameOrAfter(State.STOPPED)) {
            // Already stopping. Calling PostStart or PreStop are not supported any more.
            return;
        }

        List<Method> postStartMethods = getAnnotatedMethods(obj.getClass(), PostStart.class);
        List<Method> preStopMethods = getAnnotatedMethods(obj.getClass(), PreStop.class);

        if (currentState.isSameOrAfter(State.STOPPING)) {
            // Already started stopping (this.preStop() won't check managedInstances queue any more).
            // Need to call PreStop here. Call PostStart is not supported any more.
            invokeMethods(obj, preStopMethods);
            return;
        }

        if (currentState.isSameOrAfter(State.STARTED)) {
            // Not stopping but already has started (this.postStart() is already done).
            // Need to call PostStart here. PreStop will be called alter.
            invokeMethods(obj, postStartMethods);
            postStartMethods = ImmutableList.of();
        }

        if (!postStartMethods.isEmpty() || !preStopMethods.isEmpty()) {
            managedInstances.add(new ManagedInstance(obj, postStartMethods, preStopMethods));
        }
    }

    public void postStart()
        throws Exception
    {
        if (!state.compareAndSet(State.LATENT, State.STARTING)) {
            return;
        }

        // only one thread can enter here only once

        for (ManagedInstance managed : managedInstances) {
            invokeMethods(managed.object, managed.postStartMethods);
            if (managed.postStartCalled()) {
                managedInstances.remove(managed);
            }
        }

        state.set(State.STARTED);
    }

    public void preStop()
        throws Exception
    {
        if (!state.compareAndSet(State.STARTED, State.STOPPING)) {
            return;
        }

        // only one thread can enter here only once

        // move all instances from the queue
        List<ManagedInstance> moved = new ArrayList<>();
        while (true) {
            ManagedInstance managed = managedInstances.poll();
            if (managed == null) {
                break;
            }
            moved.add(managed);
        }

        // call in reversed order
        List<ManagedInstance> reversed = Lists.reverse(moved);
        for (ManagedInstance managed : reversed) {
            invokeMethods(managed.object, managed.preStopMethods);
        }

        state.set(State.STOPPED);
    }

    private static List<Method> getAnnotatedMethods(Class<?> clazz, Class<? extends Annotation> annotationClass)
    {
        return getAnnotatedMethodsRecursively(clazz, annotationClass, new ArrayList<>(), new HashSet<>());
    }

    private static List<Method> getAnnotatedMethodsRecursively(
            Class<?> clazz, Class<? extends Annotation> annotationClass,
            List<Method> list, Set<String> usedNames)
    {
        if (clazz == null) {
            return list;
        }

        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isSynthetic() || method.isBridge()) {
                continue;
            }

            if (method.isAnnotationPresent(annotationClass)) {
                if (!usedNames.contains(method.getName())) {
                    if (method.getParameterTypes().length != 0) {
                        throw new UnsupportedOperationException(String.format("@PostStart/@PreStop methods cannot have arguments: %s", method.getDeclaringClass().getName() + "." + method.getName() + "(...)"));
                    }

                    method.setAccessible(true);
                    usedNames.add(method.getName());
                    list.add(method);
                }
            }
        }

        getAnnotatedMethodsRecursively(clazz.getSuperclass(), annotationClass, list, usedNames);
        for (Class<?> iface : clazz.getInterfaces()) {
            getAnnotatedMethodsRecursively(iface, annotationClass, list, usedNames);
        }

        return list;
    }

    private static void invokeMethods(Object obj, List<Method> methods)
        throws Exception
    {
        for (Method method : methods) {
            try {
                method.invoke(obj);
            }
            catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
                ThrowablesUtil.propagateIfPossible(cause);
                ThrowablesUtil.propagateIfInstanceOf(cause, Exception.class);
                throw ThrowablesUtil.propagate(cause);
            }
        }
    }
}
